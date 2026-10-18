import base64
import hashlib
import hmac
import os
import re
import secrets
import time
from datetime import datetime, timedelta, timezone
from urllib.parse import quote

import uvicorn
from bson import ObjectId
from bson.errors import InvalidId
from fastapi import Depends, FastAPI, Header, HTTPException, Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from pymongo import ASCENDING, DESCENDING, MongoClient
from pymongo.errors import DuplicateKeyError, PyMongoError

# Підключення до MongoDB береться зі змінних середовища:
#   MONGODB_URI  — рядок підключення (локальний mongod або Atlas)
#   MONGODB_DB   — назва бази
ON_VERCEL = bool(os.environ.get("VERCEL"))
MONGODB_URI = os.environ.get("MONGODB_URI", "" if ON_VERCEL else "mongodb://localhost:27017")
MONGODB_DB = os.environ.get("MONGODB_DB", "codebloom")

# Fallback In-Memory implementation when MongoDB is not reachable
def apply_projection(doc: dict, proj: dict | None) -> dict:
    if not proj:
        return dict(doc)
    d = dict(doc)
    has_exclusions = any(v in (0, False) for v in proj.values())
    if has_exclusions:
        for k, v in proj.items():
            if v in (0, False) and k in d:
                del d[k]
        return d
    out = {}
    if proj.get("_id", 1) not in (0, False) and "_id" in d:
        out["_id"] = d["_id"]
    for k, v in proj.items():
        if v and k in d and k != "_id":
            out[k] = d[k]
    return out


def doc_matches(doc: dict, filt: dict | None) -> bool:
    if not filt:
        return True
    for k, v in filt.items():
        if k == "$or":
            if not isinstance(v, list) or not any(doc_matches(doc, sub) for sub in v):
                return False
            continue
        val = doc.get(k)
        if isinstance(v, dict):
            if "$in" in v:
                in_vals = {str(x) for x in v["$in"]}
                if str(val) not in in_vals and val not in v["$in"]:
                    return False
            continue
        if isinstance(v, ObjectId) or isinstance(val, ObjectId):
            if str(val) != str(v):
                return False
        elif val != v:
            return False
    return True


def apply_update_doc(doc: dict, upd: dict):
    if "$set" in upd:
        for k, v in upd["$set"].items():
            doc[k] = v
    if "$unset" in upd:
        for k in upd["$unset"]:
            doc.pop(k, None)


class InsertOneResult:
    def __init__(self, inserted_id):
        self.inserted_id = inserted_id


class UpdateResult:
    def __init__(self, modified_count):
        self.modified_count = modified_count


class DeleteResult:
    def __init__(self, deleted_count):
        self.deleted_count = deleted_count


class MemoryCursor:
    def __init__(self, docs: list, proj: dict | None = None):
        self._docs = list(docs)
        self._proj = proj

    def sort(self, key_or_list, direction=1):
        if isinstance(key_or_list, list):
            for key, direct in reversed(key_or_list):
                rev = direct in (-1, DESCENDING)
                self._docs.sort(key=lambda d: str(d.get(key, "")), reverse=rev)
        else:
            rev = direction in (-1, DESCENDING)
            self._docs.sort(key=lambda d: str(d.get(key_or_list, "")), reverse=rev)
        return self

    def limit(self, n: int):
        self._docs = self._docs[:n]
        return self

    def __iter__(self):
        for d in self._docs:
            yield apply_projection(d, self._proj)

    def __len__(self):
        return len(self._docs)


class MemoryCollection:
    def __init__(self, name: str):
        self.name = name
        self._docs = []
        self._unique_indexes = set()

    def create_index(self, keys, unique=False, **kwargs):
        if unique:
            for k, _ in keys:
                self._unique_indexes.add(k)

    def insert_one(self, doc: dict):
        d = dict(doc)
        if "_id" not in d:
            d["_id"] = ObjectId()
        for field in self._unique_indexes:
            if field in d:
                val = d[field]
                for existing in self._docs:
                    if existing.get(field) == val:
                        raise DuplicateKeyError(f"Duplicate key for field {field}: {val}")
        self._docs.append(d)
        doc["_id"] = d["_id"]
        return InsertOneResult(d["_id"])

    def find(self, filt=None, proj=None):
        matched = [d for d in self._docs if doc_matches(d, filt)]
        return MemoryCursor(matched, proj)

    def find_one(self, filt=None, proj=None):
        for d in self._docs:
            if doc_matches(d, filt):
                return apply_projection(d, proj)
        return None

    def update_one(self, filt, upd, upsert=False):
        for d in self._docs:
            if doc_matches(d, filt):
                apply_update_doc(d, upd)
                return UpdateResult(1)
        if upsert:
            new_doc = {}
            if filt:
                for k, v in filt.items():
                    if not k.startswith("$") and not isinstance(v, dict):
                        new_doc[k] = v
            apply_update_doc(new_doc, upd)
            self.insert_one(new_doc)
            return UpdateResult(1)
        return UpdateResult(0)

    def update_many(self, filt, upd):
        count = 0
        for d in self._docs:
            if doc_matches(d, filt):
                apply_update_doc(d, upd)
                count += 1
        return UpdateResult(count)

    def delete_one(self, filt):
        for i, d in enumerate(self._docs):
            if doc_matches(d, filt):
                self._docs.pop(i)
                return DeleteResult(1)
        return DeleteResult(0)

    def delete_many(self, filt):
        before = len(self._docs)
        self._docs = [d for d in self._docs if not doc_matches(d, filt)]
        return DeleteResult(before - len(self._docs))

    def count_documents(self, filt=None):
        return sum(1 for d in self._docs if doc_matches(d, filt))

    def aggregate(self, pipeline):
        sorted_msgs = sorted(self._docs, key=lambda m: str(m.get("ts", "")), reverse=True)
        grouped = {}
        for m in sorted_msgs:
            r = m.get("room")
            if r and r not in grouped:
                grouped[r] = {"_id": r, "author": m.get("author", ""), "text": m.get("text", "")}
        return list(grouped.values())


class MemoryStore:
    def __init__(self):
        self._collections = {}

    def __getitem__(self, name: str) -> MemoryCollection:
        if name not in self._collections:
            self._collections[name] = MemoryCollection(name)
        return self._collections[name]


client = None
db = None
using_mongo = False

def init_database():
    global client, db, using_mongo
    if MONGODB_URI:
        try:
            temp_client = MongoClient(
                MONGODB_URI,
                serverSelectionTimeoutMS=2500,
                connectTimeoutMS=2500,
                socketTimeoutMS=5000,
            )
            temp_client.admin.command("ping")
            client = temp_client
            db = client[MONGODB_DB]
            using_mongo = True
            print(f"[DB] Successfully connected to MongoDB ({MONGODB_DB})")
            return db
        except Exception as e:
            print(f"[DB] MongoDB ({MONGODB_URI}) unreachable: {e}. Falling back to in-memory store.")
    
    using_mongo = False
    print("[DB] Using high-performance in-memory fallback store")
    return MemoryStore()


_store = init_database()
users = _store["users"]
sessions = _store["sessions"]
rooms = _store["rooms"]
messages = _store["messages"]
topics = _store["topics"]
posts = _store["posts"]
meta = _store["meta"]
NO_AVATAR = {"avatar": 0}  # байти фото потрібні лише в /avatar/{nick}
staff = _store["staff"]  # email -> роль ("admin" | "vip"), які видає адмін

# USERS_FILE — текстовий файл зі списком користувачів (оновлюється після реєстрації)
USERS_FILE = os.environ.get(
    "USERS_FILE",
    os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "users.txt"),
)

# Головний адмін визначається за email (нікнейм можна змінювати в профілі)
ADMIN_EMAIL = os.environ.get("ADMIN_EMAIL", "artemsamaev25@gmail.com").lower()
# Ці email отримують адмінку незалежно від нікнейма (через кому, якщо їх кілька)
ADMIN_EMAILS = {
    e.strip().lower()
    for e in os.environ.get("ADMIN_EMAILS", "u.alilujko@gmail.com").split(",")
    if e.strip()
}

def export_users():
    if ON_VERCEL:
        return
    try:
        rows = list(users.find({}, {"_id": 0, "nick_name": 1, "email": 1}))
        lines = [f"{i}. {u.get('nick_name', '')} — {u.get('email', '')}" for i, u in enumerate(rows, 1)]
        with open(USERS_FILE, "w", encoding="utf-8") as f:
            f.write(f"Користувачів: {len(rows)}\n" + "\n".join(lines) + "\n")
    except Exception:
        pass

app = FastAPI(title="Code Bloom API", version="2.0.0")

FRONTEND_ORIGINS = [
    o.strip().rstrip("/")
    for o in os.environ.get(
        "FRONTEND_ORIGINS",
        "https://code-bloom-woad.vercel.app,https://code-bloom-neon.vercel.app,http://localhost:5500,http://127.0.0.1:5500,http://localhost:3000,http://127.0.0.1:3000,http://localhost:8000,http://127.0.0.1:8000",
    ).split(",")
    if o.strip()
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],
)

EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
# Нікнейм: літери (зокрема кирилиця), цифри, _ . -  — без пробілів і «/», бо нік є частиною адрес
NICK_RE = re.compile(r"^[\w.\-]{3,32}$")


def check_nick(nick: str) -> str:
    nick = nick.strip()
    if not 3 <= len(nick) <= 32:
        raise HTTPException(422, "Нікнейм має містити від 3 до 32 символів")
    if not NICK_RE.match(nick):
        raise HTTPException(422, "Нікнейм: лише літери, цифри та _ . - (без пробілів)")
    return nick


class Sign_in(BaseModel):
    login: str
    password: str


class Sign_up(BaseModel):
    login: str
    password: str
    email: str


def hash_password(password: str) -> str:
    salt = secrets.token_bytes(16)
    digest = hashlib.scrypt(password.encode(), salt=salt, n=2 ** 14, r=8, p=1)
    return f"scrypt${salt.hex()}${digest.hex()}"


def verify_password(password: str, stored: str) -> bool:
    try:
        _, salt_hex, digest_hex = stored.split("$")
        digest = hashlib.scrypt(password.encode(), salt=bytes.fromhex(salt_hex), n=2 ** 14, r=8, p=1)
    except ValueError:
        return False
    return hmac.compare_digest(digest.hex(), digest_hex)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def is_root_admin(user: dict) -> bool:
    # Головні адміни з налаштувань сервера: їх не можна забанити чи позбавити ролі з панелі.
    return user["email_lower"] == ADMIN_EMAIL or user["email_lower"] in ADMIN_EMAILS


ROLES = ("admin", "vip")
# Ранні функції: що саме відкрито для VIP та адмінів, а звичайним користувачам поки недоступне.
EARLY_FEATURES = ["vip_rooms"]


_staff_cache = {"at": 0.0, "map": {}}
STAFF_TTL = 30


def staff_map() -> dict:
    """email -> роль. Ролі потрібні майже в кожному запиті, тож тримаємо їх у пам'яті 30 с."""
    if time.monotonic() - _staff_cache["at"] > STAFF_TTL:
        _staff_cache["map"] = {d["_id"]: d["role"] for d in staff.find() if d.get("role") in ROLES}
        _staff_cache["at"] = time.monotonic()
    return _staff_cache["map"]


def staff_changed():
    _staff_cache["at"] = 0.0


def role_of(user: dict) -> str:
    """admin | vip | user. Роль видається за email (колекція staff) або налаштуваннями сервера."""
    if is_root_admin(user):
        return "admin"
    try:
        return staff_map().get(user["email_lower"], "user")
    except PyMongoError:
        return "user"


def is_admin(user: dict) -> bool:
    return role_of(user) == "admin"


def is_vip(user) -> bool:
    return bool(user) and role_of(user) in ROLES


def roles_for(nicks) -> dict:
    """{nick_lower: role} для списку нікнеймів одним запитом (для чату й форуму)."""
    keys = list({n.lower() for n in nicks})
    if not keys:
        return {}
    try:
        found = list(users.find({"nick_lower": {"$in": keys}}, {"nick_lower": 1, "email_lower": 1}))
        granted = staff_map()
    except PyMongoError:
        return {}
    return {u["nick_lower"]: "admin" if is_root_admin(u) else granted.get(u["email_lower"], "user") for u in found}


def ensure_can_write(user: dict):
    until = user.get("muted_until")
    if until:
        try:
            end = datetime.fromisoformat(until)
        except ValueError:
            return
        if end > datetime.now(timezone.utc):
            raise HTTPException(403, "Тебе заглушено до " + end.astimezone().strftime("%d.%m %H:%M"))


def new_session(user: dict) -> str:
    token = secrets.token_urlsafe(32)
    sessions.insert_one({"token": token, "nick_lower": user["nick_lower"], "created_at": datetime.now(timezone.utc)})
    return token


def current_user(authorization: str = Header(default="")) -> dict:
    token = authorization.removeprefix("Bearer ").strip()
    if not token:
        raise HTTPException(401, "Потрібно увійти")
    try:
        sess = sessions.find_one({"token": token})
        user = users.find_one({"nick_lower": sess["nick_lower"]}, NO_AVATAR) if sess else None
    except PyMongoError:
        raise HTTPException(503, "База даних недоступна")
    if not user:
        raise HTTPException(401, "Сесія недійсна, увійди знову")
    if user.get("banned"):
        raise HTTPException(403, "Акаунт заблоковано адміністратором")
    return user


def optional_user(authorization: str = Header(default="")):
    # Гість або недійсна сесія — просто None (чати й форум можна читати без входу).
    try:
        return current_user(authorization)
    except HTTPException:
        return None


def admin_user(user: dict = Depends(current_user)) -> dict:
    if not is_admin(user):
        raise HTTPException(403, "Лише для адміністратора")
    return user


def oid(value: str) -> ObjectId:
    try:
        return ObjectId(value)
    except (InvalidId, TypeError):
        raise HTTPException(404, "Не знайдено")


def seed():
    # Початковий вміст чатів і форуму — один раз, щоб видалене адміном не поверталось.
    if meta.find_one({"_id": "seeded"}):
        return
    base = datetime.now(timezone.utc)

    def ago(**kw):
        return (base - timedelta(**kw)).isoformat()

    seed_rooms = {
        "general": ("#", "Загальний чат", [("Олена", "Привіт усім! 👋"), ("Максим", "Хто сьогодні пише на Python?"), ("Іра", "Я! Саме розбираюсь із асинхронністю."), ("Олена", "Круто, поділишся досвідом на форумі?")]),
        "python": ("Py", "Python", [("Максим", "Як краще робити віртуальне середовище?"), ("Іра", "venv вистачає для більшості проєктів."), ("Олена", "А poetry — коли потрібні lock-файли.")]),
        "js": ("JS", "JavaScript", [("Дмитро", "Хтось пробував Bun замість Node?"), ("Олена", "Так, старт значно швидший.")]),
        "design": ("✦", "Дизайн", [("Софія", "Скиньте приклади гарних лендінгів."), ("Іра", "Подивись на apple.com — класика.")]),
        "help": ("?", "Допомога", [("Андрій", "Не працює flex-центрування, допоможіть."), ("Максим", "Скинь код — подивимось.")]),
        "offtop": ("☕", "Офтоп", [("Іра", "Яку музику слухаєте, коли кодите?"), ("Дмитро", "Lo-fi, як і всі 😄")]),
    }
    for rid, (icon, name, msgs) in seed_rooms.items():
        rooms.insert_one({"_id": rid, "icon": icon, "name": name})
        for i, (author, text) in enumerate(msgs):
            messages.insert_one({"room": rid, "author": author, "text": text, "ts": ago(minutes=60 - i)})

    seed_topics = [
        ("python", "Як правильно організувати проєкт на FastAPI?", "Олена", "Хочу розділити роутери, сервіси та моделі. Як це робите ви?", 24, 312, dict(hours=2),
         [("Максим", "Роби пакет app/ з підпапками routers, services, schemas. Працює чудово."), ("Іра", "Додай ще окремий шар репозиторіїв для роботи з БД.")]),
        ("js", "Bun чи Node у 2026 — що обрати?", "Дмитро", "Підкажіть, чи варто переходити на Bun для нового проєкту.", 41, 689, dict(hours=5),
         [("Олена", "Для нових проєктів Bun цілком ок, старт справді швидший.")]),
        ("design", "Приклади гарних лендінгів у стилі Apple", "Софія", "Збираю референси. Кидайте посилання!", 58, 934, dict(days=1), []),
        ("help", "Не центрується блок через flexbox", "Андрій", "justify-content: center не допомагає, у чому справа?", 6, 120, dict(days=1),
         [("Максим", "Перевір, чи в батька є висота і чи не стоїть display: block."), ("Софія", "Спробуй ще align-items: center — це друга вісь."), ("Андрій", "Допомогло, дякую!")]),
        ("offtop", "Яка музика допомагає вам кодити?", "Іра", "Я слухаю lo-fi та синтвейв.", 33, 402, dict(days=2),
         [("Дмитро", "Лише instrumental, інакше відволікаюсь.")]),
    ]
    for cat, title, author, body, likes, views, age, replies in seed_topics:
        t = topics.insert_one({"cat": cat, "title": title, "author": author, "body": body, "likes": likes, "views": views, "ts": ago(**age)})
        for author2, text in replies:
            posts.insert_one({"topic": str(t.inserted_id), "author": author2, "text": text, "ts": ago(minutes=30)})
    meta.insert_one({"_id": "seeded"})


@app.on_event("startup")
def create_indexes():
    try:
        users.create_index([("nick_lower", ASCENDING)], unique=True)
        users.create_index([("email_lower", ASCENDING)], unique=True)
    except Exception:
        pass
    try:
        sessions.create_index([("token", ASCENDING)], unique=True)
        sessions.create_index([("created_at", ASCENDING)], expireAfterSeconds=60 * 60 * 24 * 30)
        messages.create_index([("room", ASCENDING), ("ts", DESCENDING)])
        posts.create_index([("topic", ASCENDING), ("ts", ASCENDING)])
        topics.create_index([("ts", DESCENDING)])
    except Exception:
        pass
    try:
        seed()
    except Exception as e:
        print(f"[Seed warning]: {e}")
    export_users()


# Пре-сід на старті модуля для serverless (Vercel)
try:
    seed()
except Exception:
    pass


@app.get('/')
def main():
    return {
        "message": "Вітаю вас на потужному сервері Code Bloom!",
        "status": "online",
        "database": "mongodb" if using_mongo else "memory_store",
        "mongodb_connected": using_mongo
    }


@app.get("/health")
def health():
    rooms_count = 0
    topics_count = 0
    try:
        rooms_count = len(list(rooms.find()))
        topics_count = len(list(topics.find()))
    except Exception:
        pass
    return {
        "ok": True,
        "database": "mongodb" if using_mongo else "memory_store",
        "mongodb_connected": using_mongo,
        "rooms_count": rooms_count,
        "topics_count": topics_count
    }


@app.post("/sign_up", status_code=201)
def sign_up(data: Sign_up):
    nick = check_nick(data.login)
    email = data.email.strip()

    if not EMAIL_RE.match(email):
        raise HTTPException(422, "Некоректний email")
    if len(data.password) < 8:
        raise HTTPException(422, "Пароль має містити мінімум 8 символів")

    try:
        users.create_index([("nick_lower", ASCENDING)], unique=True)
        users.create_index([("email_lower", ASCENDING)], unique=True)
        doc = {
            "nick_name": nick,
            "nick_lower": nick.lower(),
            "email": email,
            "email_lower": email.lower(),
            "password": hash_password(data.password),
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        users.insert_one(doc)
        token = new_session(doc)
    except DuplicateKeyError:
        raise HTTPException(409, "Такий нікнейм або email уже зайнятий")
    except PyMongoError:
        raise HTTPException(503, "База даних недоступна")

    export_users()
    return {"nick_name": nick, "token": token, "is_admin": is_admin(doc), "role": role_of(doc), "avatar": None}


@app.post("/sign_in")
def sign_in(data: Sign_in):
    login = data.login.strip().lower()

    try:
        user = users.find_one({"$or": [{"nick_lower": login}, {"email_lower": login}]}, NO_AVATAR)
    except PyMongoError:
        raise HTTPException(503, "База даних недоступна")

    if not user or not verify_password(data.password, user["password"]):
        raise HTTPException(401, "Невірний логін або пароль")
    if user.get("banned"):
        raise HTTPException(403, "Акаунт заблоковано адміністратором")

    try:
        token = new_session(user)
    except PyMongoError:
        raise HTTPException(503, "База даних недоступна")

    return {"nick_name": user["nick_name"], "token": token, "is_admin": is_admin(user), "role": role_of(user),
            "avatar": avatar_path(user)}


@app.get("/profile/{nick}")
def profile(nick: str):
    try:
        user = users.find_one({"nick_lower": nick.strip().lower()}, NO_AVATAR)
    except PyMongoError:
        raise HTTPException(503, "База даних недоступна")

    if not user:
        raise HTTPException(404, "Користувача не знайдено")

    return {
        "nick_name": user["nick_name"],
        "email": user["email"],
        "created_at": user.get("created_at"),
        "is_admin": is_admin(user),
        "role": role_of(user),
        "avatar": avatar_path(user),
    }


def me_out(user: dict) -> dict:
    role = role_of(user)
    return {"nick_name": user["nick_name"], "email": user["email"], "created_at": user.get("created_at"),
            "role": role, "is_admin": role == "admin", "avatar": avatar_path(user),
            "features": EARLY_FEATURES if role in ROLES else []}


@app.get("/me")
def me(user: dict = Depends(current_user)):
    return me_out(user)


# ---------- Редагування профілю ----------
class ProfileIn(BaseModel):
    nick_name: str


class AvatarIn(BaseModel):
    image: str  # data:image/jpeg;base64,...


AVATAR_MAX = 400 * 1024
AVATAR_TYPES = {"jpeg": b"\xff\xd8\xff", "png": b"\x89PNG", "webp": b"RIFF"}


def avatar_path(user: dict):
    # Шлях відносно API; ?v= змінюється з кожним новим фото, щоб браузер не показував старе.
    if not user.get("avatar_ver"):
        return None
    return "/avatar/" + quote(user["nick_name"]) + "?v=" + str(user["avatar_ver"])


@app.patch("/me")
def update_profile(data: ProfileIn, user: dict = Depends(current_user)):
    new = check_nick(data.nick_name)
    old = user["nick_name"]
    if new == old:
        return me_out(user)

    def run():
        users.update_one({"_id": user["_id"]}, {"$set": {"nick_name": new, "nick_lower": new.lower()}})
        # Сесії й авторство повідомлень, тем і відповідей переходять на новий нік
        sessions.update_many({"nick_lower": user["nick_lower"]}, {"$set": {"nick_lower": new.lower()}})
        for coll in (messages, topics, posts):
            coll.update_many({"author": old}, {"$set": {"author": new}})
        return users.find_one({"_id": user["_id"]}, NO_AVATAR)

    try:
        fresh = run()
    except DuplicateKeyError:
        raise HTTPException(409, "Такий нікнейм уже зайнятий")
    except PyMongoError:
        raise HTTPException(503, "База даних недоступна")
    export_users()
    return me_out(fresh)


@app.put("/me/avatar")
def upload_avatar(data: AvatarIn, user: dict = Depends(current_user)):
    m = re.match(r"^data:image/(jpeg|png|webp);base64,(.+)$", data.image, re.S)
    if not m:
        raise HTTPException(422, "Потрібне зображення JPG, PNG або WebP")
    try:
        raw = base64.b64decode(m.group(2), validate=True)
    except ValueError:
        raise HTTPException(422, "Пошкоджене зображення")
    kind = m.group(1)
    if not raw.startswith(AVATAR_TYPES[kind]):
        raise HTTPException(422, "Файл не схожий на зображення")
    if len(raw) > AVATAR_MAX:
        raise HTTPException(413, "Фото завелике (максимум 400 КБ після стиснення)")
    ver = int(time.time() * 1000)
    db_call(lambda: users.update_one({"_id": user["_id"]},
                                     {"$set": {"avatar": raw, "avatar_type": "image/" + kind, "avatar_ver": ver}}))
    user.update(avatar_ver=ver)
    return me_out(user)


@app.delete("/me/avatar")
def delete_avatar(user: dict = Depends(current_user)):
    db_call(lambda: users.update_one({"_id": user["_id"]}, {"$unset": {"avatar": "", "avatar_type": "", "avatar_ver": ""}}))
    user.pop("avatar_ver", None)
    return me_out(user)


@app.get("/avatar/{nick}")
def get_avatar(nick: str):
    u = db_call(lambda: users.find_one({"nick_lower": nick.strip().lower()}, {"avatar": 1, "avatar_type": 1}))
    if not u or not u.get("avatar"):
        raise HTTPException(404, "Аватар не знайдено")
    # Адреса містить ?v=версію, тож кешувати можна надовго
    return Response(content=bytes(u["avatar"]), media_type=u.get("avatar_type", "image/jpeg"),
                    headers={"Cache-Control": "public, max-age=31536000, immutable"})


class RoomIn(BaseModel):
    name: str
    icon: str = "#"
    vip_only: bool = False


class StaffIn(BaseModel):
    email: str
    role: str


class MuteIn(BaseModel):
    minutes: int


class BanIn(BaseModel):
    banned: bool


class TextIn(BaseModel):
    text: str


class MessageIn(BaseModel):
    text: str
    reply_to: str | None = None  # id повідомлення, на яке відповідають


class TopicIn(BaseModel):
    cat: str
    title: str
    body: str = ""


CATS = {"python", "js", "design", "help", "offtop"}


def clean(value: str, limit: int, what: str) -> str:
    value = value.strip()
    if not value or len(value) > limit:
        raise HTTPException(422, f"{what}: від 1 до {limit} символів")
    return value


def db_call(fn):
    try:
        return fn()
    except HTTPException:
        raise
    except PyMongoError as e:
        if using_mongo:
            raise HTTPException(503, "База даних недоступна")
        raise HTTPException(500, f"Помилка сховища: {e}")


# ---------- Чат ----------
def get_room(room_id: str, user):
    room = rooms.find_one({"_id": room_id})
    if not room:
        raise HTTPException(404, "Чат не знайдено")
    if room.get("vip_only") and not is_vip(user):
        raise HTTPException(403, "Ця кімната лише для VIP")
    return room


@app.get("/rooms")
def list_rooms(user: dict = Depends(optional_user)):
    def run():
        vip = is_vip(user)
        # Останнє повідомлення кожної кімнати — одним запитом, а не окремим на кожну кімнату
        lasts = {d["_id"]: d for d in messages.aggregate([
            {"$sort": {"room": 1, "ts": -1}},
            {"$group": {"_id": "$room", "author": {"$first": "$author"}, "text": {"$first": "$text"}}},
        ])}
        out = []
        for r in rooms.find():
            if r.get("vip_only") and not vip:
                continue
            last = lasts.get(r["_id"])
            out.append({"id": r["_id"], "icon": r["icon"], "name": r["name"], "vip_only": bool(r.get("vip_only")),
                        "last": {"author": last["author"], "text": last["text"]} if last else None})
        return out
    return db_call(run)


def msg_out(m, roles, by_id=None):
    out = {"id": str(m["_id"]), "author": m["author"], "text": m["text"], "ts": m["ts"],
           "role": roles.get(m["author"].lower(), "user"), "edited": m.get("edited_at")}
    rid = m.get("reply_to")
    if rid:
        # Цитата береться з поточного стану: якщо оригінал змінили чи видалили, це видно
        r = (by_id or {}).get(rid)
        out["reply"] = {"id": rid, "author": r["author"], "text": r["text"][:200]} if r else {"id": rid, "deleted": True}
    return out


@app.get("/rooms/{room_id}/messages")
def room_messages(room_id: str, user: dict = Depends(optional_user)):
    def run():
        get_room(room_id, user)
        rows = list(messages.find({"room": room_id}).sort("ts", -1).limit(200))
        by_id = {str(m["_id"]): m for m in rows}
        # Оригінали відповідей, яких немає серед останніх 200, — одним запитом
        missing = {m["reply_to"] for m in rows if m.get("reply_to") and m["reply_to"] not in by_id}
        if missing:
            ids = [ObjectId(x) for x in missing if ObjectId.is_valid(x)]
            by_id.update({str(m["_id"]): m for m in messages.find({"_id": {"$in": ids}})})
        roles = roles_for({m["author"] for m in rows})
        return [msg_out(m, roles, by_id) for m in reversed(rows)]
    return db_call(run)


@app.post("/rooms/{room_id}/messages", status_code=201)
def send_message(room_id: str, data: MessageIn, user: dict = Depends(current_user)):
    text = clean(data.text, 1000, "Повідомлення")
    ensure_can_write(user)

    def run():
        get_room(room_id, user)
        doc = {"room": room_id, "author": user["nick_name"], "text": text, "ts": now_iso()}
        by_id = {}
        if data.reply_to:
            target = messages.find_one({"_id": oid(data.reply_to), "room": room_id})
            if not target:
                raise HTTPException(404, "Повідомлення, на яке ти відповідаєш, не знайдено")
            doc["reply_to"] = str(target["_id"])
            by_id[doc["reply_to"]] = target
        messages.insert_one(doc)
        return msg_out(doc, {user["nick_lower"]: role_of(user)}, by_id)
    return db_call(run)


def own_message(message_id: str, user: dict) -> dict:
    m = messages.find_one({"_id": oid(message_id)})
    if not m:
        raise HTTPException(404, "Повідомлення не знайдено")
    get_room(m["room"], user)
    return m


@app.patch("/messages/{message_id}")
def edit_message(message_id: str, data: TextIn, user: dict = Depends(current_user)):
    text = clean(data.text, 1000, "Повідомлення")
    ensure_can_write(user)

    def run():
        m = own_message(message_id, user)
        if m["author"] != user["nick_name"]:
            raise HTTPException(403, "Редагувати можна лише свої повідомлення")
        edited = now_iso()
        messages.update_one({"_id": m["_id"]}, {"$set": {"text": text, "edited_at": edited}})
        m.update(text=text, edited_at=edited)
        return msg_out(m, {user["nick_lower"]: role_of(user)})
    return db_call(run)


@app.delete("/messages/{message_id}")
def delete_message(message_id: str, user: dict = Depends(current_user)):
    def run():
        m = own_message(message_id, user)
        if m["author"] != user["nick_name"] and not is_admin(user):
            raise HTTPException(403, "Видаляти можна лише свої повідомлення")
        messages.delete_one({"_id": m["_id"]})
    db_call(run)
    return {"ok": True}


# ---------- Форум ----------
def topic_out(t, replies, roles):
    def role(name):
        return roles.get(name.lower(), "user")
    return {"id": str(t["_id"]), "cat": t["cat"], "title": t["title"], "author": t["author"], "body": t["body"],
            "likes": t["likes"], "views": t["views"], "ts": t["ts"], "role": role(t["author"]),
            "posts": [{"id": str(p["_id"]), "author": p["author"], "text": p["text"], "ts": p["ts"],
                       "role": role(p["author"])} for p in replies]}


@app.get("/topics")
def list_topics():
    def run():
        replies = {}
        for p in posts.find().sort("ts", 1):
            replies.setdefault(p["topic"], []).append(p)
        rows = list(topics.find().sort("ts", -1))
        names = {t["author"] for t in rows} | {p["author"] for ps in replies.values() for p in ps}
        roles = roles_for(names)
        return [topic_out(t, replies.get(str(t["_id"]), []), roles) for t in rows]
    return db_call(run)


@app.post("/topics", status_code=201)
def create_topic(data: TopicIn, user: dict = Depends(current_user)):
    if data.cat not in CATS:
        raise HTTPException(422, "Невідомий розділ")
    ensure_can_write(user)
    doc = {"cat": data.cat, "title": clean(data.title, 150, "Заголовок"), "author": user["nick_name"],
           "body": data.body.strip()[:5000] or "Без опису.", "likes": 0, "views": 0, "ts": now_iso()}
    db_call(lambda: topics.insert_one(doc))
    return topic_out(doc, [], {user["nick_lower"]: role_of(user)})


@app.post("/topics/{topic_id}/posts", status_code=201)
def reply_topic(topic_id: str, data: TextIn, user: dict = Depends(current_user)):
    text = clean(data.text, 2000, "Відповідь")
    ensure_can_write(user)

    def run():
        if not topics.find_one({"_id": oid(topic_id)}):
            raise HTTPException(404, "Тему не знайдено")
        doc = {"topic": topic_id, "author": user["nick_name"], "text": text, "ts": now_iso()}
        posts.insert_one(doc)
        return {"id": str(doc["_id"]), "author": doc["author"], "text": doc["text"], "ts": doc["ts"],
                "role": role_of(user)}
    return db_call(run)


# ---------- Адмін-панель ----------
@app.get("/admin/me")
def admin_me(user: dict = Depends(admin_user)):
    return {"nick_name": user["nick_name"]}


@app.post("/admin/rooms", status_code=201)
def admin_add_room(data: RoomIn, _: dict = Depends(admin_user)):
    room = {"_id": secrets.token_hex(4), "name": clean(data.name, 40, "Назва"), "icon": clean(data.icon or "#", 4, "Іконка"),
            "vip_only": data.vip_only}
    db_call(lambda: rooms.insert_one(room))
    return {"id": room["_id"], "icon": room["icon"], "name": room["name"], "vip_only": room["vip_only"], "last": None}


@app.delete("/admin/rooms/{room_id}")
def admin_delete_room(room_id: str, _: dict = Depends(admin_user)):
    def run():
        if rooms.delete_one({"_id": room_id}).deleted_count == 0:
            raise HTTPException(404, "Чат не знайдено")
        messages.delete_many({"room": room_id})
    db_call(run)
    return {"ok": True}


@app.delete("/admin/messages/{message_id}")
def admin_delete_message(message_id: str, _: dict = Depends(admin_user)):
    if db_call(lambda: messages.delete_one({"_id": oid(message_id)})).deleted_count == 0:
        raise HTTPException(404, "Повідомлення не знайдено")
    return {"ok": True}


@app.delete("/admin/topics/{topic_id}")
def admin_delete_topic(topic_id: str, _: dict = Depends(admin_user)):
    def run():
        if topics.delete_one({"_id": oid(topic_id)}).deleted_count == 0:
            raise HTTPException(404, "Тему не знайдено")
        posts.delete_many({"topic": topic_id})
    db_call(run)
    return {"ok": True}


@app.delete("/admin/posts/{post_id}")
def admin_delete_post(post_id: str, _: dict = Depends(admin_user)):
    if db_call(lambda: posts.delete_one({"_id": oid(post_id)})).deleted_count == 0:
        raise HTTPException(404, "Відповідь не знайдено")
    return {"ok": True}


def view_user(u, granted):
    root = is_root_admin(u)
    role = "admin" if root else granted.get(u["email_lower"], "user")
    return {"nick_name": u["nick_name"], "email": u["email"], "created_at": u.get("created_at"),
            "role": role, "is_admin": role == "admin", "protected": root,
            "banned": bool(u.get("banned")), "muted_until": u.get("muted_until")}


@app.get("/admin/users")
def admin_users(_: dict = Depends(admin_user)):
    def run():
        granted = {d["_id"]: d["role"] for d in staff.find()}
        return [view_user(u, granted) for u in users.find({}, {"avatar": 0})]
    return db_call(run)


def target_user(nick: str, me: dict) -> dict:
    """Користувач, до якого застосовують дію модерації (не себе і не іншого адміна)."""
    key = nick.strip().lower()
    if key == me["nick_lower"]:
        raise HTTPException(400, "Не можна застосувати до власного акаунта")
    user = db_call(lambda: users.find_one({"nick_lower": key}, NO_AVATAR))
    if not user:
        raise HTTPException(404, "Користувача не знайдено")
    if is_admin(user):
        raise HTTPException(403, "Не можна застосувати до адміністратора")
    return user


@app.delete("/admin/users/{nick}")
def admin_delete_user(nick: str, me: dict = Depends(admin_user)):
    user = target_user(nick, me)

    def run():
        users.delete_one({"_id": user["_id"]})
        sessions.delete_many({"nick_lower": user["nick_lower"]})
    db_call(run)
    export_users()
    return {"ok": True}


@app.post("/admin/users/{nick}/ban")
def admin_ban_user(nick: str, data: BanIn, me: dict = Depends(admin_user)):
    user = target_user(nick, me)

    def run():
        users.update_one({"_id": user["_id"]}, {"$set": {"banned": data.banned}})
        if data.banned:
            sessions.delete_many({"nick_lower": user["nick_lower"]})  # вихід з усіх пристроїв
    db_call(run)
    return {"ok": True, "banned": data.banned}


@app.post("/admin/users/{nick}/mute")
def admin_mute_user(nick: str, data: MuteIn, me: dict = Depends(admin_user)):
    if not 0 <= data.minutes <= 60 * 24 * 365:
        raise HTTPException(422, "Мут: від 0 (зняти) до 525600 хвилин")
    user = target_user(nick, me)
    until = (datetime.now(timezone.utc) + timedelta(minutes=data.minutes)).isoformat() if data.minutes else None
    db_call(lambda: users.update_one({"_id": user["_id"]}, {"$set": {"muted_until": until}}))
    return {"ok": True, "muted_until": until}


# ---------- Ролі: адмін додає email в адміни або VIP ----------
@app.get("/admin/staff")
def admin_staff(_: dict = Depends(admin_user)):
    def run():
        by_email = {u["email_lower"]: u["nick_name"] for u in users.find({}, {"email_lower": 1, "nick_name": 1})}
        return [{"email": d["_id"], "role": d["role"], "nick_name": by_email.get(d["_id"])} for d in staff.find()]
    return db_call(run)


@app.post("/admin/staff", status_code=201)
def admin_add_staff(data: StaffIn, _: dict = Depends(admin_user)):
    email = data.email.strip().lower()
    if not EMAIL_RE.match(email):
        raise HTTPException(422, "Некоректний email")
    if data.role not in ROLES:
        raise HTTPException(422, "Роль: admin або vip")
    db_call(lambda: staff.update_one({"_id": email}, {"$set": {"role": data.role}}, upsert=True))
    staff_changed()
    return {"email": email, "role": data.role}


@app.delete("/admin/staff/{email}")
def admin_remove_staff(email: str, _: dict = Depends(admin_user)):
    key = email.strip().lower()
    if db_call(lambda: staff.delete_one({"_id": key})).deleted_count == 0:
        raise HTTPException(404, "Цього email немає в списку ролей")
    staff_changed()
    return {"ok": True}


if __name__ == '__main__':
    uvicorn.run(app)

