"""Python-порт lab.js: те же контрольные данные, проверки и сохранение результатов.

Учебные функции находятся только в solution.py. Команды: docker compose run --rm py help.
Перенесены все команды комплекта: reset, fixtures, data, index, purchase, failover и all.
"""
import asyncio
import hashlib
import importlib
import json
import os
import sys
from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pymongo
from bson import Decimal128, json_util
from bson.json_util import CANONICAL_JSON_OPTIONS, RELAXED_JSON_OPTIONS
from pymongo import AsyncMongoClient, monitoring
from pymongo.asynchronous.cursor import AsyncCursor
from pymongo.asynchronous.database import AsyncDatabase
from pymongo.errors import OperationFailure, PyMongoError
from pymongo.results import UpdateResult
from pymongo.server_type import SERVER_TYPE
from pymongo.write_concern import WriteConcern

NAME = "hw2"
DATASET_VERSION = "2026-10-05"
SERVER_VERSION = "8.0.30"
WC = WriteConcern(w="majority", j=True, wtimeout=10000)
OUTPUT = Path("/out/results.json")
SOLUTION = Path(__file__).with_name("solution.py")
# Тот же формат файла, что у lab.js: canonical Extended JSON.
CANONICAL = CANONICAL_JSON_OPTIONS.with_options(tz_aware=True, tzinfo=timezone.utc)


def iso(text: str) -> datetime:
    return datetime.fromisoformat(text)


def now() -> datetime:
    return datetime.now(timezone.utc)


FROM = iso("2026-01-01T00:00:00Z")
TO = iso("2026-02-01T00:00:00Z")


# --- Контрольные данные (HW.* из lab.js). Целые Python в диапазоне int32 пишутся как BSON int32.


def products() -> list[dict]:
    return [
        {
            "_id": sku,
            "name": name,
            "category": category,
            "price": Decimal128(price),
            "variants": [{"size": "L", "stock": l_stock}, {"size": "M", "stock": m_stock}],
        }
        for sku, name, category, price, l_stock, m_stock in [
            ("A", "Футболка", "clothes", "100.00", 0, 10),
            ("B", "Рубашка", "clothes", "80.00", 5, 4),
            ("C", "Толстовка", "clothes", "90.00", 3, 2),
            ("D", "Ремень", "accessories", "120.00", 4, 6),
            ("E", "Сумка", "accessories", "200.00", 2, 1),
            ("F", "Шарф", "accessories", "150.00", 1, 8),
        ]
    ]


def item(sku: str, size: str, qty: int, price: str) -> dict:
    return {"sku": sku, "size": size, "qty": qty, "price": Decimal128(price)}


def order(
    order_id: str,
    date: str | datetime,
    items: list[dict],
    status: str = "paid",
    user_id: int = 2500,
) -> dict:
    created_at = date if isinstance(date, datetime) else iso(date)
    return {
        "_id": order_id,
        "userId": user_id,
        "createdAt": created_at,
        "status": status,
        "items": items,
    }


def orders() -> list[dict]:
    return [
        order("sale-start", "2026-01-01T00:00:00Z", [item("A", "M", 2, "100.00"), item("B", "L", 4, "80.00")]),
        order("sale-middle", "2026-01-15T12:00:00Z", [item("A", "L", 1, "120.00"), item("C", "M", 3, "90.00")]),
        order(
            "sale-last",
            "2026-01-31T23:59:59.999Z",
            [item("D", "L", 2, "120.00"), item("E", "M", 1, "200.00"), item("F", "L", 1, "150.00")],
        ),
        order("excluded-end", "2026-02-01T00:00:00Z", [item("A", "M", 999, "100.00")]),
        order("excluded-before", "2025-12-31T23:59:59.999Z", [item("A", "M", 999, "100.00")]),
        order("excluded-created", "2026-01-15T00:00:00Z", [item("A", "M", 999, "100.00")], "created"),
        order("excluded-cancelled", "2026-01-15T00:00:00Z", [item("A", "M", 999, "100.00")], "cancelled"),
        *(
            order(f"H{i:02d}", datetime(2026, 3, 1, i // 2, tzinfo=timezone.utc), [item("F", "M", 1, "150.00")])
            for i in range(26)
        ),
        order("other-user", "2026-04-01T00:00:00Z", [item("F", "M", 1, "150.00")], "paid", 2501),
        order("other-status", "2026-04-01T00:00:00Z", [item("F", "M", 1, "150.00")], "cancelled"),
    ]


def large_orders() -> list[dict]:
    start = datetime(2026, 5, 1, tzinfo=timezone.utc)
    return [
        order(
            f"G{i:05d}",
            start + timedelta(minutes=i // 2),
            [item("F", "M", 1, "150.00")],
            "paid" if i % 4 < 2 else "created",
            2500 if i % 100 < 30 else 3000 + i % 71,
        )
        for i in range(10000)
    ]


SEARCH_EXPECTED = [
    ("clothes", "L", 2, ["B", "C"]),
    ("clothes", "M", 5, ["A"]),
    ("accessories", "L", 3, ["D"]),
    ("clothes", "L", 99, []),
]
HISTORY_EXPECTED = [
    "H24", "H25", "H22", "H23", "H20", "H21", "H18", "H19", "H16", "H17",
    "H14", "H15", "H12", "H13", "H10", "H11", "H08", "H09", "H06", "H07",
]  # fmt: skip
REPORT_EXPECTED = [
    {"sku": "A", "name": "Футболка", "qty": 3, "revenue": "320.00"},
    {"sku": "B", "name": "Рубашка", "qty": 4, "revenue": "320.00"},
    {"sku": "C", "name": "Толстовка", "qty": 3, "revenue": "270.00"},
    {"sku": "D", "name": "Ремень", "qty": 2, "revenue": "240.00"},
    {"sku": "E", "name": "Сумка", "qty": 1, "revenue": "200.00"},
]


async def reset(client: AsyncMongoClient, with_orders: bool = True) -> AsyncDatabase:
    # Только специально выделенная база в отдельном Compose-проекте.
    await client.drop_database(NAME)
    db = client[NAME]
    await db.create_collection("products")
    await db.create_collection("orders")
    await db.get_collection("products", write_concern=WC).insert_many(products())
    if with_orders:
        await db.get_collection("orders", write_concern=WC).insert_many(orders())
    return db


# --- Вывод, сравнение и сохранение результатов.


def log(label: str, value: Any = None) -> None:
    print("\n" + label)
    if value is not None:
        print(json_util.dumps(value, json_options=RELAXED_JSON_OPTIONS, indent=2, ensure_ascii=False))


def normalized(value: Any) -> Any:
    # Сравнение через relaxed Extended JSON: int32/int64 равны как числа, порядок ключей не важен.
    return json.loads(json_util.dumps(value, json_options=RELAXED_JSON_OPTIONS))


def product_fields(p: dict) -> dict:
    return {
        "_id": p["_id"],
        "name": p["name"],
        "category": p["category"],
        "price": p["price"],
        "variants": [{"size": v["size"], "stock": v["stock"]} for v in p["variants"]],
    }


def order_fields(o: dict) -> dict:
    return {
        "_id": o["_id"],
        "userId": o["userId"],
        "createdAt": o["createdAt"],
        "status": o["status"],
        "items": [{"sku": i["sku"], "size": i["size"], "qty": i["qty"], "price": i["price"]} for i in o["items"]],
    }


def ids(documents: list[dict]) -> list:
    return [d["_id"] for d in documents]


async def collection_options(db: AsyncDatabase, name: str) -> dict:
    cursor = await db.list_collections(filter={"name": name})
    return (await cursor.to_list())[0]["options"]


async def state(db: AsyncDatabase) -> dict:
    return {
        "products": await db.products.find({}).sort("_id", 1).to_list(),
        "orders": await db.orders.find({}).sort("_id", 1).to_list(),
    }


def compact(s: dict) -> dict:
    return {
        "stocks": [{"sku": p["_id"], "variants": p["variants"]} for p in s["products"]],
        "orderCount": len(s["orders"]),
        "orderIds": ids(s["orders"]),
    }


async def error_code(operation: Awaitable[Any]) -> Any:
    try:
        await operation
    except Exception as e:
        return getattr(e, "code", None)
    return None


def has_stage(plan: Any, stage: str) -> bool:
    if isinstance(plan, list):
        return any(has_stage(x, stage) for x in plan)
    if not isinstance(plan, dict):
        return False
    return plan.get("stage") == stage or any(has_stage(v, stage) for v in plan.values())


async def explain(cursor: AsyncCursor) -> dict:
    # AsyncCursor.explain() запрашивает только verbosity allPlansExecution. Для executionStats
    # команда explain собирается из того же курсора: фильтр, сортировка, limit и hint.
    command: dict[str, Any] = {"find": cursor.collection.name, "filter": cursor._spec}
    for key, value in (("sort", cursor._ordering), ("limit", cursor._limit), ("hint", cursor._hint)):
        if value:
            command[key] = value
    return await cursor.collection.database.command("explain", command, verbosity="executionStats")


def write_result(result: UpdateResult) -> dict:
    # Результат записи драйвера в виде документа — для лога и results.json.
    return {
        "acknowledged": result.acknowledged,
        "matchedCount": result.matched_count,
        "modifiedCount": result.modified_count,
        "upsertedId": result.upserted_id,
        "raw": result.raw_result,
    }


# Журнал драйвера для опыта со сменой primary: что клиент узнал сам, без replSetGetStatus.
# События SDAM (ServerListener) PyMongo доставляет из очереди раз в секунду, поэтому роли
# берутся из ответов мониторинга (heartbeat): они публикуются сразу, с точным временем.
DRIVER_EVENTS: list[dict] = []


class RoleMonitor(monitoring.ServerHeartbeatListener):
    """Роль узла по каждому ответу мониторинга (hello); в журнал попадают только смены роли.

    awaited=True — streaming-мониторинг: узел отвечает на ожидающий hello сразу,
    как только меняется его состояние, не дожидаясь очередного опроса.
    """

    def __init__(self) -> None:
        self.roles: dict[str, str] = {}

    def started(self, event: monitoring.ServerHeartbeatStartedEvent) -> None:
        pass

    def succeeded(self, event: monitoring.ServerHeartbeatSucceededEvent) -> None:
        host, port = event.connection_id
        address, role = f"{host}:{port}", SERVER_TYPE._fields[event.reply.server_type]
        if self.roles.get(address) != role:
            self.roles[address] = role
            DRIVER_EVENTS.append({"at": now(), "event": "hello", "host": address, "role": role, "awaited": event.awaited})

    def failed(self, event: monitoring.ServerHeartbeatFailedEvent) -> None:
        host, port = event.connection_id
        self.roles.pop(f"{host}:{port}", None)
        DRIVER_EVENTS.append(
            {"at": now(), "event": "helloFailed", "host": f"{host}:{port}", "error": str(event.reply), "awaited": event.awaited}
        )


class CommandFailures(monitoring.CommandListener):
    """Отказы команд. Повтор внутри одной операции (retryable reads/writes) приложение
    не видит; здесь он виден как отказ команды перед её успешным повтором.
    """

    def started(self, event: monitoring.CommandStartedEvent) -> None:
        pass

    def succeeded(self, event: monitoring.CommandSucceededEvent) -> None:
        pass

    def failed(self, event: monitoring.CommandFailedEvent) -> None:
        host, port = event.connection_id
        DRIVER_EVENTS.append(
            {"at": now(), "event": "commandFailed", "host": f"{host}:{port}", "command": event.command_name, "failure": event.failure}
        )


def driver_view(client: AsyncMongoClient) -> dict:
    # Роли узлов глазами драйвера: результат его собственного мониторинга (SDAM), а не replSetGetStatus.
    description = client.topology_description
    return {
        "topologyType": description.topology_type_name,
        "servers": [
            {"host": f"{host}:{port}", "type": server.server_type_name}
            for (host, port), server in sorted(description.server_descriptions().items())
        ],
    }


# Снимки снимаются с primary вне транзакционной сессии.
# Не запускайте параллельно другие сценарии и не переключайте primary.
async def snapshot_transactions(client: AsyncMongoClient) -> dict:
    s = await client.admin.command({"serverStatus": 1, "transactions": 1})
    if s.get("ok") != 1 or not s.get("transactions"):
        raise RuntimeError("serverStatus.transactions unavailable")
    return {
        "host": s["host"],
        "pid": s["pid"],
        "uptime": s["uptime"],
        "localTime": s["localTime"],
        "transactions": s["transactions"],
    }


class Lab:
    """Состояние запуска: клиент, загруженное решение и файл results.json."""

    def __init__(self, client: AsyncMongoClient):
        self.client = client
        self.solution: Any = None
        self.current: dict = {}
        OUTPUT.parent.mkdir(parents=True, exist_ok=True)
        if OUTPUT.exists():
            self.results = json_util.loads(OUTPUT.read_text("utf-8"), json_options=CANONICAL)
        else:
            self.results = {"dataset": DATASET_VERSION, "scenarios": {}}

    def persist(self) -> None:
        self.results["updatedAt"] = now()
        tmp = OUTPUT.with_name(OUTPUT.name + ".tmp")
        text = json_util.dumps(self.results, json_options=CANONICAL, indent=2, ensure_ascii=False)
        tmp.write_text(text + "\n", "utf-8")
        tmp.replace(OUTPUT)

    def save(self, name: str, value: Any) -> None:
        self.current["artifacts"][name] = value
        self.persist()

    def check(self, name: str, condition: bool) -> None:
        if not condition:
            raise AssertionError("FAIL: " + name)
        self.current["checks"].append(name)
        self.persist()
        print("PASS " + name)

    def equal(self, name: str, a: Any, b: Any) -> None:
        self.check(name, normalized(a) == normalized(b))

    async def measure_transactions(
        self, scenario: str, operation: Callable[[], Awaitable[Any]]
    ) -> tuple[Any, Exception | None, dict]:
        before = await snapshot_transactions(self.client)
        result, error = None, None
        try:
            result = await operation()
        except Exception as e:
            error = e
        after = await snapshot_transactions(self.client)
        same_process = (
            before["host"] == after["host"] and before["pid"] == after["pid"] and after["uptime"] >= before["uptime"]
        )
        delta = {
            key: after["transactions"][key] - before["transactions"][key]
            for key in ("totalStarted", "totalCommitted", "totalAborted")
        }
        evidence = {"scenario": scenario, "before": before, "after": after, "delta": delta, "sameProcess": same_process}
        self.save("transactions-" + scenario, evidence)
        if not same_process:
            raise RuntimeError("Повторите опыт: primary или процесс изменился между снимками")
        # Отличие от +1 обсудите в README: например, при повторной попытке транзакции.
        # Эти снимки дополняют проверку данных и кода, но сами по себе её не заменяют.
        return result, error, evidence


# --- Сценарии.


async def data_scenario(lab: Lab) -> None:
    solution = lab.solution
    db = await reset(lab.client)
    await solution.install_validator(db)
    validator_options = await collection_options(db, "products")
    lab.check(
        "validator strict/error installed",
        bool(validator_options.get("validator"))
        and validator_options.get("validationLevel") == "strict"
        and validator_options.get("validationAction") == "error",
    )
    validation = []
    for doc_id, mutate, expected in [
        ("valid", lambda p: p, 0),
        ("string-price", lambda p: {**p, "price": "100.00"}, 121),
        ("negative-stock", lambda p: {**p, "variants": [{"size": "L", "stock": -1}]}, 121),
    ]:
        product = mutate({**products()[0], "_id": doc_id})
        code, details = 0, None
        try:
            await db.products.insert_one(product)
        except OperationFailure as e:
            code, details = e.code, (e.details or {}).get("errInfo")
        lab.check("validation " + doc_id, code == expected)
        validation.append({"id": doc_id, "code": code, "details": details})
        if not code:
            await db.products.delete_one({"_id": doc_id})
    options = await collection_options(db, "products")

    searches = []
    for category, size, qty, expected in SEARCH_EXPECTED:
        query = {"category": category, "size": size, "qty": qty}
        label = f"{category}/{size}/{qty}"
        documents = await solution.search_products(db, query)
        found = ids(documents)
        expected_documents = await db.products.find({"_id": {"$in": expected}}).sort("_id", 1).to_list()
        lab.equal(
            "complete product documents " + label,
            [product_fields(d) for d in documents],
            [product_fields(d) for d in expected_documents],
        )
        lab.equal("search " + label, found, expected)
        searches.append({**query, "documents": documents, "found": found, "expected": expected})
    await db.products.update_one({"_id": "A", "variants.size": "L"}, {"$set": {"variants.$.stock": 2}})
    changed_documents = await solution.search_products(db, {"category": "clothes", "size": "L", "qty": 2})
    changed = ids(changed_documents)
    lab.equal("L=2 includes A", changed, ["A", "B", "C"])
    searches.append({"afterChange": "A/L=2", "documents": changed_documents, "found": changed})

    history_documents = await solution.history(db, {"userId": 2500, "status": "paid"})
    h = ids(history_documents)
    fixture_orders = {o["_id"]: o for o in orders()}
    lab.equal(
        "complete history documents",
        [order_fields(o) for o in history_documents],
        [order_fields(fixture_orders[i]) for i in HISTORY_EXPECTED],
    )
    lab.equal("history 20 IDs including date ties", h, HISTORY_EXPECTED)
    other_user = await solution.history(db, {"userId": 2501, "status": "paid"})
    other_status = await solution.history(db, {"userId": 2500, "status": "cancelled"})
    no_history = await solution.history(db, {"userId": 9999, "status": "paid"})
    lab.equal("history other user", ids(other_user), ["other-user"])
    lab.equal("history other status", ids(other_status), ["other-status", "excluded-cancelled"])
    lab.equal("history no matches", no_history, [])
    artifact = {
        "options": options,
        "validation": validation,
        "searches": searches,
        "history": history_documents,
        "historyIds": h,
        "otherUser": other_user,
        "otherStatus": other_status,
        "noHistory": no_history,
        "historyExpected": HISTORY_EXPECTED,
    }
    # В отличие от lab.js, результаты раздела 2 сохраняются до отчёта: пока report — TODO,
    # сценарий на нём останавливается, а подтверждения валидатора и запросов уже в файле.
    lab.save("data", {**artifact, "checks": lab.current["checks"]})

    def table(rows: list[dict]) -> list[dict]:
        return [{"sku": r["sku"], "name": r["name"], "qty": int(r["qty"]), "revenue": str(r["revenue"])} for r in rows]

    full_report = await solution.report(db, FROM, TO)
    actual = table(full_report)
    lab.equal("report top 5 amounts/order/names", actual, REPORT_EXPECTED)
    empty_report = {"from": iso("2024-01-01T00:00:00Z"), "to": iso("2024-02-01T00:00:00Z")}
    empty_report["rows"] = await solution.report(db, empty_report["from"], empty_report["to"])
    lab.equal("empty interval", empty_report["rows"], [])
    boundary_report = {"from": FROM, "to": iso("2026-01-01T00:00:00.001Z")}
    boundary_report["rows"] = await solution.report(db, boundary_report["from"], boundary_report["to"])
    lab.equal(
        "from included",
        [[x["sku"], x["revenue"]] for x in table(boundary_report["rows"])],
        [["B", "320.00"], ["A", "200.00"]],
    )
    await db.products.update_one({"_id": "A"}, {"$set": {"price": Decimal128("999.00")}})
    after_price_report = {"from": FROM, "to": TO, "changedProduct": {"sku": "A", "price": Decimal128("999.00")}}
    after_price_report["rows"] = await solution.report(db, FROM, TO)
    lab.equal("catalog price does not rewrite revenue", table(after_price_report["rows"]), actual)
    lab.save(
        "data",
        {
            **artifact,
            "report": full_report,
            "reportParameters": {"from": FROM, "to": TO},
            "emptyReport": empty_report,
            "boundaryReport": boundary_report,
            "afterPriceReport": after_price_report,
            "expected": REPORT_EXPECTED,
            "checks": lab.current["checks"],
        },
    )
    log("REPORT: [2026-01-01, 2026-02-01), paid", actual)
    log("REPORT: empty interval", empty_report)
    log("REPORT: included from boundary", boundary_report)
    log("REPORT: after catalog price change", after_price_report)
    log("HISTORY: userId=2500, status=paid", h)


async def index_scenario(lab: Lab) -> None:
    solution = lab.solution
    query = {"userId": 2500, "status": "paid"}
    db = await reset(lab.client, with_orders=False)
    fixture = large_orders()
    await db.get_collection("orders", write_concern=WC).insert_many(fixture)
    lab.equal("10000 documents", await db.orders.count_documents({}), 10000)
    lab.equal("only _id index initially", list(await db.orders.index_information()), ["_id_"])
    before_documents = await solution.history(db, query)
    before_ids = ids(before_documents)
    before = await explain(solution.history_cursor(db, query))
    history_index = getattr(solution, "HISTORY_INDEX", None)
    if not history_index or not isinstance(history_index, dict):
        raise NotImplementedError("TODO: HISTORY_INDEX")
    await db.orders.create_index(history_index, name="history_user_status_date_id")
    after = await explain(solution.history_cursor(db, query))
    after_documents = await solution.history(db, query)
    after_ids = ids(after_documents)
    lab.equal("same IDs and order", after_ids, before_ids)
    # Независимый расчёт в Python: устойчивая сортировка по _id, затем по createdAt по убыванию.
    matching = sorted((o for o in fixture if o["userId"] == 2500 and o["status"] == "paid"), key=lambda o: o["_id"])
    expected = ids(sorted(matching, key=lambda o: o["createdAt"], reverse=True)[:20])
    lab.equal("matches independent fixture order", after_ids, expected)

    def row(plan: dict) -> dict:
        stats = plan["executionStats"]
        return {
            "nReturned": stats["nReturned"],
            "totalKeysExamined": stats["totalKeysExamined"],
            "totalDocsExamined": stats["totalDocsExamined"],
            "hasSORT": has_stage(plan["queryPlanner"]["winningPlan"], "SORT"),
            "executionTimeMillis": stats["executionTimeMillis"],
        }

    summary = {
        "filter": query,
        "sort": {"createdAt": -1, "_id": 1},
        "index": history_index,
        "hint": solution.USES_HISTORY_HINT,
        "before": row(before),
        "after": row(after),
        "beforeIds": before_ids,
        "afterIds": after_ids,
        "beforeDocuments": before_documents,
        "afterDocuments": after_documents,
    }
    lab.check(
        "before scans collection + SORT",
        has_stage(before["queryPlanner"]["winningPlan"], "COLLSCAN") and summary["before"]["hasSORT"],
    )
    lab.check(
        "after uses IXSCAN without SORT",
        has_stage(after["queryPlanner"]["winningPlan"], "IXSCAN") and not summary["after"]["hasSORT"],
    )
    lab.check("examines fewer documents", summary["after"]["totalDocsExamined"] < summary["before"]["totalDocsExamined"])
    lab.save("explain-before", before)
    lab.save("explain-after", after)
    lab.save("index", summary)
    log("INDEX", {key: summary[key] for key in ("filter", "sort", "index", "hint", "before", "after")})


async def purchase_scenario(lab: Lab) -> None:
    solution = lab.solution
    db = await reset(lab.client, with_orders=False)
    await solution.install_validator(db)
    request = {
        "operationId": "purchase-001",
        "userId": 2500,
        "items": [{"sku": "A", "size": "M", "qty": 2}, {"sku": "B", "size": "L", "qty": 1}],
    }
    trace: list[dict] = []

    def emit(event: dict) -> None:
        trace.append(event)
        log("TX", event)

    # Нехватка только второго товара: первое изменение реально проходит в транзакции.
    initial = await state(db)
    rollback_request = {
        **request,
        "operationId": "rollback-001",
        "items": [request["items"][0], {"sku": "B", "size": "L", "qty": 6}],
    }
    _, rollback_exception, rollback_evidence = await lab.measure_transactions(
        "rollback", lambda: solution.purchase(db, rollback_request, emit)
    )
    rollback_error = getattr(rollback_exception, "code", None)
    rolled_back = await state(db)
    lab.check("second item insufficient", rollback_error == "ITEM_UNAVAILABLE")
    lab.check("first update modified before abort", bool(trace) and trace[0]["sku"] == "A" and trace[0]["modified"] == 1)
    lab.check(
        "server observed started and aborted transaction",
        rollback_evidence["delta"]["totalStarted"] >= 1 and rollback_evidence["delta"]["totalAborted"] >= 1,
    )
    lab.equal("abort restores both full documents and leaves no order", rolled_back, initial)
    log("AFTER ABORT", compact(rolled_back))
    rollback = {
        "request": rollback_request,
        "trace": list(trace),
        "error": rollback_error,
        "before": initial,
        "after": rolled_back,
    }
    trace.clear()

    order, success_exception, success_evidence = await lab.measure_transactions(
        "success", lambda: solution.purchase(db, request, emit)
    )
    if success_exception is not None:
        raise success_exception
    lab.check(
        "server observed started and committed transaction",
        success_evidence["delta"]["totalStarted"] >= 1 and success_evidence["delta"]["totalCommitted"] >= 1,
    )
    success = await state(db)
    expected = products()
    expected[0]["variants"][1]["stock"] = 8
    expected[1]["variants"][0]["stock"] = 4
    lab.equal(
        "success changes only selected stocks",
        [product_fields(p) for p in success["products"]],
        [product_fields(p) for p in expected],
    )
    lab.check(
        "exactly one created order",
        len(success["orders"]) == 1 and order["_id"] == request["operationId"] and order["status"] == "created",
    )
    lab.check(
        "userId/date types",
        isinstance(order["userId"], int)
        and order["userId"] == request["userId"]
        and isinstance(order["createdAt"], datetime),
    )
    lab.equal("snapshot item prices", [str(x["price"]) for x in order["items"]], ["100.00", "80.00"])
    lab.equal(
        "saved items match request in original order",
        [{"sku": x["sku"], "size": x["size"], "qty": int(x["qty"])} for x in order["items"]],
        request["items"],
    )
    lab.equal("returned order is saved order", order, success["orders"][0])
    stored_types = await (
        await db.orders.aggregate(
            [
                {"$match": {"_id": request["operationId"]}},
                {
                    "$project": {
                        "userId": {"$type": "$userId"},
                        "createdAt": {"$type": "$createdAt"},
                        "items": {
                            "$map": {
                                "input": "$items",
                                "as": "item",
                                "in": {"qty": {"$type": "$$item.qty"}, "price": {"$type": "$$item.price"}},
                            }
                        },
                    }
                },
            ]
        )
    ).to_list()
    type_row = stored_types[0] if stored_types else None
    correct_types = (
        type_row is not None
        and type_row["userId"] in ("int", "long")
        and type_row["createdAt"] == "date"
        and all(t["qty"] in ("int", "long") and t["price"] == "decimal" for t in type_row["items"])
    )
    lab.check("stored BSON types: integer user/qty, decimal prices, date", correct_types)
    log("SUCCESS", compact(success))
    log("SAVED ORDER", order)

    # Цена каталога меняется, порядок ключей запроса меняется: это всё ещё тот же запрос.
    await db.products.update_one({"_id": "A"}, {"$set": {"price": Decimal128("777.00")}})
    before_replay = await state(db)
    replay_request = {
        "userId": 2500,
        "items": [{"qty": i["qty"], "size": i["size"], "sku": i["sku"]} for i in request["items"]],
        "operationId": request["operationId"],
    }
    replayed = await solution.purchase(db, replay_request, emit)
    lab.equal("replay returns original order", replayed, order)
    after_replay = await state(db)
    lab.equal("replay no writes despite changed catalog price/key order", after_replay, before_replay)
    log("REPLAY: same order, stocks unchanged", compact(after_replay))
    conflict_error = await error_code(
        solution.purchase(db, {**request, "items": [{**request["items"][0], "qty": 3}, request["items"][1]]}, emit)
    )
    lab.check("changed qty rejected as request conflict", conflict_error == "REQUEST_CONFLICT")
    after_conflict = await state(db)
    lab.equal("conflict changes nothing", after_conflict, after_replay)
    # Отсутствующий вариант после первого успешного изменения также отменяет покупку.
    missing_error = await error_code(
        solution.purchase(
            db,
            {**request, "operationId": "missing-001", "items": [request["items"][0], {"sku": "B", "size": "XXL", "qty": 1}]},
            emit,
        )
    )
    lab.check("missing variant rejected", missing_error == "ITEM_UNAVAILABLE")
    lab.equal("missing variant rollback", await state(db), after_replay)
    unknown_error = await error_code(
        solution.purchase(
            db,
            {
                **request,
                "operationId": "unknown-001",
                "items": [request["items"][0], {"sku": "NO_SUCH_SKU", "size": "L", "qty": 1}],
            },
            emit,
        )
    )
    lab.check("unknown SKU rejected", unknown_error == "ITEM_UNAVAILABLE")
    lab.equal("unknown SKU rollback", await state(db), after_replay)
    lab.save(
        "purchase",
        {
            "request": request,
            "rollback": rollback,
            "success": {"order": order, "state": success},
            "replay": {"order": replayed, "before": before_replay, "after": after_replay},
            "conflict": {"error": conflict_error, "after": after_conflict},
            "missingError": missing_error,
            "unknownError": unknown_error,
            "trace": trace,
            "checks": lab.current["checks"],
        },
    )


async def failover_scenario(lab: Lab) -> None:
    solution = lab.solution
    client = lab.client  # Единственный экземпляр на весь опыт, seed list + replicaSet.
    await client.drop_database("hw2_failover")
    db = client["hw2_failover"]
    await db.create_collection("probe")

    async def roles() -> list[dict]:
        status = await client.admin.command("replSetGetStatus")
        return [{"host": m["name"], "role": m["stateStr"]} for m in status["members"]]

    def count(members: list[dict], role: str) -> int:
        return sum(1 for m in members if m["role"] == role)

    before = await roles()
    lab.check(
        "3 data-bearing nodes ready",
        len(before) == 3 and count(before, "PRIMARY") == 1 and count(before, "SECONDARY") == 2,
    )
    old_primary = next(m["host"] for m in before if m["role"] == "PRIMARY")
    driver_before = driver_view(client)
    first_write = write_result(await solution.write_probe(db, "before", "majority confirmed before stepDown"))
    lab.check("first majority write acknowledged", first_write["acknowledged"] is True)
    log("ROLES BEFORE", before)
    log("FIRST WRITE", first_write)
    events_from = len(DRIVER_EVENTS)
    step_down_at = now()
    try:
        step_down = await client.admin.command("replSetStepDown", 60, secondaryCatchUpPeriodSecs=10)
    except PyMongoError as e:
        step_down = {"message": str(e), "code": getattr(e, "code", None)}
    retries: list[dict] = []
    after = read = new_write = None
    for attempt in range(1, 61):
        try:
            after = await roles()
            primary = next((m for m in after if m["role"] == "PRIMARY"), None)
            if primary is None or primary["host"] == old_primary or count(after, "SECONDARY") != 2:
                raise RuntimeError("Waiting for changed primary and two secondaries")
            read = await solution.read_probe(db, "before")
            # upsert делает повтор этой тестовой записи безопасным при потере ответа.
            new_write = write_result(await solution.write_probe(db, "after", "majority confirmed after stepDown"))
            break
        except Exception as e:
            retries.append({"attempt": attempt, "error": str(e)})
            await asyncio.sleep(0.5)
    switched_at = now()
    driver_after = driver_view(client)
    driver_events = DRIVER_EVENTS[events_from:]
    lab.check(
        "primary actually changed",
        after is not None and any(m["role"] == "PRIMARY" and m["host"] != old_primary for m in after),
    )
    lab.check("same client instance", db.client is client and client is lab.client)
    lab.check("old majority document read", (read or {}).get("message") == "majority confirmed before stepDown")
    lab.check("new majority write acknowledged", new_write is not None and new_write["acknowledged"] is True)
    lab.equal("both documents present", await db.probe.count_documents({}), 2)
    result = {
        "connection": os.environ["MONGODB_URI"],
        "sameClient": True,
        "writeConcern": WC.document,
        "before": before,
        "driverBefore": driver_before,
        "firstWrite": first_write,
        "stepDownAt": step_down_at,
        "stepDown": step_down,
        "retries": retries,
        "switchedAt": switched_at,
        "after": after,
        "driverAfter": driver_after,
        "driverEvents": driver_events,
        "read": read,
        "newWrite": new_write,
        "documents": await db.probe.find({}).sort("_id", 1).to_list(),
        "checks": lab.current["checks"],
    }
    lab.save("failover", result)
    log("ROLES AFTER", after)
    log("DRIVER VIEW BEFORE/AFTER", {"before": driver_before, "after": driver_after})
    log("DRIVER EVENTS SINCE stepDown", driver_events)
    log("READ OLD DOCUMENT", read)
    log("NEW WRITE", new_write)


async def reset_scenario(lab: Lab) -> None:
    mode = sys.argv[2] if len(sys.argv) > 2 else "data"
    if mode not in ("data", "index"):
        raise ValueError("Используйте py reset или py reset index")
    db = await reset(lab.client, mode == "data")
    if mode == "index":
        await db.get_collection("orders", write_concern=WC).insert_many(large_orders())
    product_count = await db.products.count_documents({})
    order_count = await db.orders.count_documents({})
    lab.save("reset", {"datasetVersion": DATASET_VERSION, "mode": mode, "products": product_count, "orders": order_count})
    lab.check("fixtures loaded", product_count == 6 and order_count == (10000 if mode == "index" else 35))
    log("DATASET", {"mode": mode, "products": product_count, "orders": order_count})


HANDLERS = {
    "reset": reset_scenario,
    "data": data_scenario,
    "index": index_scenario,
    "purchase": purchase_scenario,
    "failover": failover_scenario,
}
SCENARIOS = ["data", "index", "purchase", "failover"]

HELP = """MongoDB — домашняя работа, вариант на Python (PyMongo, AsyncMongoClient)
  docker compose up -d                      Поднять три узла и инициализировать replica set
  docker compose build py                   Пересобрать образ после изменения зависимостей
  docker compose run --rm py reset          Восстановить 6 товаров и 35 заказов; сбросить старые результаты
  docker compose run --rm py reset index    Загрузить 10 000 заказов для опыта с индексом (без solution.py)
  docker compose run --rm py fixtures       Показать контрольные данные, параметры и эталоны без решения
  docker compose run --rm py data           Проверить валидатор, поиск, историю и отчёт
  docker compose run --rm py index          Сравнить один запрос до/после индекса на 10 000 заказов
  docker compose run --rm py purchase       Проверить покупку, откат, повтор и конфликт
  docker compose run --rm py failover       Сменить primary, прочитать и записать тем же клиентом
  docker compose run --rm py all            Последовательно выполнить четыре проверки
  docker compose run --rm lab shell         Открыть mongosh для своих запросов
  docker compose down                       Остановить стенд, сохранив данные
  docker compose down --volumes             Удалить контейнеры и данные этого стенда

Каждый сценарий сам готовит свои данные. Выполняйте команды последовательно.
Все результаты и полные explain: results/results.json. Ошибка заменяет прежний PASS.
При изменении solution.py старые результаты отмечаются устаревшими при следующем запуске.
Базы стенда: hw2, hw2_failover. Пока TODO не заполнены, проверки завершаются ошибкой.
URI внутри Docker-сети стенда:
mongodb://m1:27017,m2:27017,m3:27017/hw2?replicaSet=hw2rs&retryWrites=true&serverSelectionTimeoutMS=10000&readPreference=primary"""


def fixtures() -> None:
    log("PRODUCTS", products())
    log("ORDERS", orders())
    log(
        "SEARCH EXPECTED",
        [
            {"input": {"category": category, "size": size, "qty": qty}, "ids": expected}
            for category, size, qty, expected in SEARCH_EXPECTED
        ],
    )
    log("HISTORY EXPECTED", {"input": {"userId": 2500, "status": "paid"}, "ids": HISTORY_EXPECTED})
    log("REPORT EXPECTED", {"from": FROM, "to": TO, "rows": REPORT_EXPECTED})


async def run(scenario: str, client: AsyncMongoClient) -> None:
    lab = Lab(client)
    scenarios = lab.results["scenarios"]
    solution_hash = hashlib.sha256(SOLUTION.read_bytes()).hexdigest()
    for entry in scenarios.values():
        if entry.get("solutionHash") != solution_hash and entry.get("status") in ("passed", "failed"):
            entry["previousStatus"] = entry["status"]
            entry["status"] = "outdated"
    if scenario == "reset":
        for name in SCENARIOS:
            scenarios[name] = {"status": "not_run"}
    selected = SCENARIOS if scenario == "all" else [scenario]
    for name in selected:
        scenarios[name] = {"status": "pending"}
    lab.persist()
    for name in selected:
        current = lab.current = scenarios[name] = {
            "status": "running",
            "startedAt": now(),
            "solutionHash": solution_hash,
            "checks": [],
            "artifacts": {},
        }
        lab.persist()
        try:
            current["server"] = (await client.server_info())["version"]
            current["driver"] = f"PyMongo {pymongo.version} (AsyncMongoClient)"
            if current["server"] != SERVER_VERSION:
                raise RuntimeError(f"Нужна MongoDB {SERVER_VERSION}, получена {current['server']}")
            if name != "reset" and lab.solution is None:
                lab.solution = importlib.import_module("solution")
            log(
                "RUN " + name,
                {"dataset": DATASET_VERSION, "startedAt": current["startedAt"], "server": current["server"]},
            )
            await HANDLERS[name](lab)
            current["status"] = "passed"
            current["finishedAt"] = now()
            lab.persist()
            log("COMPLETE " + name, {"passed": len(current["checks"]), "results": "results/results.json"})
        except Exception as e:
            current["status"] = "failed"
            current["finishedAt"] = now()
            current["error"] = {"message": str(e), "code": getattr(e, "code", None)}
            lab.persist()
            raise


async def main() -> None:
    scenario = sys.argv[1] if len(sys.argv) > 1 else "help"
    if scenario == "help":
        print(HELP)
        return
    if scenario == "fixtures":
        fixtures()
        return
    if scenario not in HANDLERS and scenario != "all":
        raise SystemExit("Неизвестная команда. Выполните docker compose run --rm py help")
    listeners = [RoleMonitor(), CommandFailures()]
    async with AsyncMongoClient(os.environ["MONGODB_URI"], tz_aware=True, event_listeners=listeners) as client:
        await run(scenario, client)


if __name__ == "__main__":
    asyncio.run(main())
