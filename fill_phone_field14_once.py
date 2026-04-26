import asyncio
import logging
import re
from typing import Any

from config import conf_logger
from pyrus_api_service import api_request

FORM_ID = 2351567
SOURCE_FIELD_ID = 27
TARGET_FIELD_ID = 47
EMPTY_VALUE = "-"
UPDATE_CONCURRENCY = 50

logger = logging.getLogger(__name__)


def _extract_phone_raw(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, dict):
        for key in ("text", "value", "plain"):
            inner = value.get(key)
            if inner is not None:
                return str(inner).strip()
        return ""
    return str(value).strip()


def normalize_phone(value: Any) -> str:
    raw = _extract_phone_raw(value)
    if not raw:
        return EMPTY_VALUE

    digits = re.sub(r"\D+", "", raw)
    if not digits:
        return EMPTY_VALUE

    if len(digits) == 11 and digits.startswith("8"):
        return "7" + digits[1:]
    if len(digits) == 10 and digits.startswith("9"):
        return "7" + digits
    if len(digits) == 11 and digits.startswith("7"):
        return digits
    if digits.startswith("375") and len(digits) >= 10:
        return digits

    # Остальные случаи считаем невалидным телефоном для этой миграции.
    return EMPTY_VALUE


def _field_value_by_id(fields: list[dict[str, Any]], field_id: int) -> Any:
    for field in fields:
        if field.get("id") == field_id:
            return field.get("value")
    return None


async def fetch_tasks() -> list[dict[str, Any]]:
    endpoint = f"/forms/{FORM_ID}/register?fld{TARGET_FIELD_ID}=empty&fld{SOURCE_FIELD_ID}=*"
    logger.info("Fetch tasks endpoint=%s", endpoint)
    result = await api_request(
        method="GET",
        endpoint=endpoint,
    )
    if not isinstance(result, dict):
        return []
    tasks = result.get("tasks") or []
    if not isinstance(tasks, list):
        return []
    return [t for t in tasks if isinstance(t, dict)]


async def update_task_field14(task_id: int, normalized_value: str, source_raw: str) -> None:
    payload = {
        "text": f"[one-time script] auto-filled field {TARGET_FIELD_ID} from field {SOURCE_FIELD_ID}: {source_raw!r} -> {normalized_value!r}",
        "field_updates": [
            {"id": TARGET_FIELD_ID, "value": normalized_value},
        ],
        "skip_notification": "true"
    }
    await api_request(
        method="POST",
        endpoint=f"/tasks/{task_id}/comments",
        json_data=payload,
    )


async def process_task(task: dict[str, Any]) -> str:
    raw_task_id = task.get("id")
    if raw_task_id is None:
        logger.warning("Skip task without id: %r", task)
        return "skipped"

    try:
        task_id = int(raw_task_id)
    except (TypeError, ValueError):
        logger.warning("Skip task with non-int id: %r", raw_task_id)
        return "skipped"

    fields = task.get("fields") or []
    if not isinstance(fields, list):
        fields = []

    source_value = _field_value_by_id(fields, SOURCE_FIELD_ID)
    source_raw = _extract_phone_raw(source_value)
    normalized = normalize_phone(source_value)

    try:
        await update_task_field14(task_id, normalized, source_raw)
        logger.info(
            "Updated task_id=%s field%s=%r -> field%s=%r",
            task_id,
            SOURCE_FIELD_ID,
            source_raw,
            TARGET_FIELD_ID,
            normalized,
        )
        return "updated"
    except Exception as exc:
        logger.exception(
            "Failed update task_id=%s source=%r normalized=%r error=%r",
            task_id,
            source_raw,
            normalized,
            exc,
        )
        return "failed"


async def main() -> None:
    conf_logger()
    logger.info(
        "Start one-time fill: form=%s source_field=%s target_field=%s",
        FORM_ID,
        SOURCE_FIELD_ID,
        TARGET_FIELD_ID,
    )

    processed = 0
    updated = 0
    skipped = 0
    failed = 0

    tasks = await fetch_tasks()
    logger.info("Fetched tasks: %s", len(tasks))

    concurrency = min(max(1, UPDATE_CONCURRENCY), 100)
    logger.info("Update concurrency=%s", concurrency)
    for i in range(0, len(tasks), concurrency):
        chunk = tasks[i : i + concurrency]
        processed += len(chunk)
        chunk_updated = 0
        chunk_skipped = 0
        chunk_failed = 0
        chunk_done = 0

        chunk_tasks = [
            asyncio.create_task(process_task(task))
            for task in chunk
        ]
        for finished in asyncio.as_completed(chunk_tasks):
            status = await finished
            chunk_done += 1
            if status == "updated":
                chunk_updated += 1
            elif status == "skipped":
                chunk_skipped += 1
            else:
                chunk_failed += 1

            if chunk_done % 10 == 0 or chunk_done == len(chunk):
                logger.info(
                    "Chunk progress: start=%s done=%s/%s updated=%s skipped=%s failed=%s",
                    i,
                    chunk_done,
                    len(chunk),
                    chunk_updated,
                    chunk_skipped,
                    chunk_failed,
                )

        updated += chunk_updated
        skipped += chunk_skipped
        failed += chunk_failed
        logger.info(
            "Chunk done: start=%s size=%s processed=%s updated=%s skipped=%s failed=%s",
            i,
            len(chunk),
            processed,
            updated,
            skipped,
            failed,
        )

    logger.info(
        "Done one-time fill. processed=%s updated=%s skipped=%s failed=%s",
        processed,
        updated,
        skipped,
        failed,
    )


if __name__ == "__main__":
    asyncio.run(main())
