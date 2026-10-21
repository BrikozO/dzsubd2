"""Учебные функции — Python-аналог solution.js.

db — переданная база (AsyncDatabase); функции выполняют реальные запросы MongoDB.
Входы — словари с ключами контракта: {category, size, qty}, {userId, status} и
{operationId, userId, items: [{sku, size, qty}, {sku, size, qty}]}.
Служебный lab.py менять не требуется.
"""
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from typing import Any, Literal, TypedDict

from pymongo import ASCENDING, DESCENDING, ReadPreference
from pymongo.asynchronous.client_session import AsyncClientSession
from pymongo.asynchronous.cursor import AsyncCursor
from pymongo.asynchronous.database import AsyncDatabase
from pymongo.read_concern import ReadConcern
from pymongo.results import UpdateResult
from pymongo.write_concern import WriteConcern

Product = dict[str, Any]
Order = dict[str, Any]


class SearchInput(TypedDict):
    category: str
    size: str
    qty: int


class HistoryInput(TypedDict):
    userId: int
    status: Literal["created", "paid", "cancelled"]


class PurchaseItem(TypedDict):
    sku: str
    size: str
    qty: int


class PurchaseInput(TypedDict):
    operationId: str
    userId: int
    items: list[PurchaseItem]


# Дополнительные служебные поля разрешены заданием, поэтому additionalProperties не задан.
# Целые — int32 или int64; minimum сравнивает числа независимо от BSON-типа.
PRODUCTS_VALIDATOR = {
    "$jsonSchema": {
        "bsonType": "object",
        "required": ["_id", "name", "category", "price", "variants"],
        "properties": {
            "_id": {"bsonType": "string"},
            "name": {"bsonType": "string"},
            "category": {"bsonType": "string"},
            "price": {"bsonType": "decimal", "minimum": 0},
            "variants": {
                "bsonType": "array",
                "items": {
                    "bsonType": "object",
                    "required": ["size", "stock"],
                    "properties": {
                        "size": {"bsonType": "string"},
                        "stock": {"bsonType": ["int", "long"], "minimum": 0},
                    },
                },
            },
        },
    }
}

HISTORY_LIMIT = 20

# Индекс для history_cursor по правилу ESR: сначала поля равенства userId и status, затем поля
# сортировки в её порядке и направлениях; _id упорядочивает заказы с равным createdAt.
HISTORY_INDEX = {"userId": ASCENDING, "status": ASCENDING, "createdAt": DESCENDING, "_id": ASCENDING}
# hint в history_cursor не используется: планировщик выбирает индекс сам.
USES_HISTORY_HINT = False

# Подтверждение записи стенда (приложение А): большинство узлов и журнал.
WC = WriteConcern(w="majority", j=True, wtimeout=10000)


# Коллекция products уже создана (reset), поэтому валидатор ставится через collMod.
async def install_validator(db: AsyncDatabase) -> dict[str, Any]:
    return await db.command(
        "collMod",
        "products",
        validator=PRODUCTS_VALIDATOR,
        validationLevel="strict",
        validationAction="error",
    )


# Возврат: список полных Product; порядок _id ASC; [] при отсутствии.
# $elemMatch привязывает размер и остаток к одному и тому же варианту.
async def search_products(db: AsyncDatabase, query: SearchInput) -> list[Product]:
    cursor = db.products.find(
        {
            "category": query["category"],
            "variants": {
                "$elemMatch": {"size": query["size"], "stock": {"$gte": query["qty"]}}
            },
        }
    ).sort("_id", ASCENDING)
    return await cursor.to_list()


# Технический helper: курсор с фильтром, сортировкой и limit.
# Один и тот же запрос используется для результата и explain в опыте с индексом.
def history_cursor(db: AsyncDatabase, query: HistoryInput) -> AsyncCursor:
    return (
        db.orders.find({"userId": query["userId"], "status": query["status"]})
        .sort([("createdAt", DESCENDING), ("_id", ASCENDING)])
        .limit(HISTORY_LIMIT)
    )


# Возврат: не более 20 полных Order; createdAt DESC, затем _id ASC; [] при отсутствии.
async def history(db: AsyncDatabase, query: HistoryInput) -> list[Order]:
    return await history_cursor(db, query).to_list()


# Возврат: список {sku, name, qty, revenue}; revenue — Decimal128.
# Весь расчёт — в pipeline. Название подтягивается после $limit: $lookup нужен только пяти строкам.
async def report(db: AsyncDatabase, start: datetime, end: datetime) -> list[dict[str, Any]]:
    cursor = await db.orders.aggregate(
        [
            {"$match": {"status": "paid", "createdAt": {"$gte": start, "$lt": end}}},
            {"$unwind": "$items"},
            {
                "$group": {
                    "_id": "$items.sku",
                    "qty": {"$sum": "$items.qty"},
                    "revenue": {"$sum": {"$multiply": ["$items.qty", "$items.price"]}},
                }
            },
            {"$sort": {"revenue": -1, "_id": 1}},
            {"$limit": 5},
            {"$lookup": {"from": "products", "localField": "_id", "foreignField": "_id", "as": "product"}},
            {"$unwind": "$product"},
            {"$project": {"_id": 0, "sku": "$_id", "name": "$product.name", "qty": "$qty", "revenue": "$revenue"}},
        ]
    )
    return await cursor.to_list()


class HWError(Exception):
    """Отказ покупки: код отделён от текста сообщения, как HWError в lab.js."""

    def __init__(self, code: str, message: str | None = None) -> None:
        super().__init__(message or code)
        self.code = code


# Оболочка транзакции — перенос transaction() из lab.js: одна сессия, одна транзакция.
# Исключение из body → abort и проброс; успешный body → commit. Повторов нет: ошибку commit
# не выдаём за успешный abort — исход может быть неопределённым.
async def transaction[T](db: AsyncDatabase, body: Callable[[AsyncClientSession], Awaitable[T]]) -> T:
    async with db.client.start_session(causal_consistency=True) as session:
        await session.start_transaction(
            read_concern=ReadConcern("snapshot"),
            write_concern=WC,
            read_preference=ReadPreference.PRIMARY,
        )
        try:
            result = await body(session)
        except BaseException:
            await session.abort_transaction()
            raise
        await session.commit_transaction()
        return result


# Параметры бизнес-запроса для сравнения с сохранённым заказом: значения, а не текст JSON.
# Порядок позиций значим; порядок ключей, int32/int64 и текущая цена каталога — нет.
def purchase_params(user_id: int, items: list[dict[str, Any]]) -> tuple[int, list[tuple[str, str, int]]]:
    return user_id, [(item["sku"], item["size"], item["qty"]) for item in items]


# request: {operationId, userId, items: [{sku, size, qty}, {sku, size, qty}]}; порядок items значим.
# Возврат: созданный или ранее сохранённый Order.
# При отказе: HWError("ITEM_UNAVAILABLE") или HWError("REQUEST_CONFLICT"), transaction() делает abort.
# Каждая операция получает session: без неё запись выполнилась бы вне транзакции.
async def purchase(
    db: AsyncDatabase,
    request: PurchaseInput,
    trace: Callable[[dict[str, Any]], None] = lambda event: None,
) -> Order:
    async def body(session: AsyncClientSession) -> Order:
        saved = await db.orders.find_one({"_id": request["operationId"]}, session=session)
        if saved is not None:
            if purchase_params(saved["userId"], saved["items"]) != purchase_params(request["userId"], request["items"]):
                raise HWError("REQUEST_CONFLICT")
            return saved
        items = []
        for item in request["items"]:
            # Достаточный остаток — часть условия: найти вариант и списать — одна атомарная операция.
            result = await db.products.update_one(
                {
                    "_id": item["sku"],
                    "variants": {"$elemMatch": {"size": item["size"], "stock": {"$gte": item["qty"]}}},
                },
                {"$inc": {"variants.$.stock": -item["qty"]}},
                session=session,
            )
            trace(
                {
                    "event": "update_inside_transaction",
                    "sku": item["sku"],
                    "matched": result.matched_count,
                    "modified": result.modified_count,
                }
            )
            if result.modified_count != 1:
                raise HWError("ITEM_UNAVAILABLE")
            product = await db.products.find_one({"_id": item["sku"]}, {"price": 1}, session=session)
            items.append({"sku": item["sku"], "size": item["size"], "qty": item["qty"], "price": product["price"]})
        now = datetime.now(timezone.utc)
        order = {
            "_id": request["operationId"],
            "userId": request["userId"],
            # BSON Date хранит миллисекунды: возвращаемый заказ совпадает с сохранённым.
            "createdAt": now.replace(microsecond=now.microsecond // 1000 * 1000),
            "status": "created",
            "items": items,
        }
        await db.orders.insert_one(order, session=session)
        return order

    return await transaction(db, body)


# Используйте именно переданный db: он связан с одним AsyncMongoClient опыта.
# Запись {_id, message} в probe с majority и j:true. upsert делает повтор с тем же id
# безопасным: оболочка опыта может повторить запись после ошибки связи.
async def write_probe(db: AsyncDatabase, probe_id: str, message: str) -> UpdateResult:
    probe = db.get_collection("probe", write_concern=WC)
    return await probe.update_one({"_id": probe_id}, {"$set": {"message": message}}, upsert=True)


# Возврат — документ probe с указанным _id либо None. Чтение с primary — readPreference клиента.
async def read_probe(db: AsyncDatabase, probe_id: str) -> dict[str, Any] | None:
    return await db.probe.find_one({"_id": probe_id})
