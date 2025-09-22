
from __future__ import annotations
import logging
import hashlib
from datetime import datetime
from typing import Dict, Any

log = logging.getLogger(__name__)

def iso_now() -> str:
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"

def checksum(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:12]

def add_lineage(context: Dict[str, Any], dataset_in: str, dataset_out: str) -> Dict[str, str]:
    """
    Minimal lineage payload you can forward to OpenLineage or your logging sink.
    """
    dag_id = context["dag"].dag_id
    run_id = context["run_id"]
    ti = context["ti"]
    return {
        "timestamp": iso_now(),
        "dag_id": dag_id,
        "run_id": run_id,
        "task_id": ti.task_id,
        "dataset_in": dataset_in,
        "dataset_out": dataset_out,
    }

def assert_idempotent(existing_rows: int, incoming_rows: int) -> None:
    """
    Example contract: incoming rows must be >= 0 and not explode unexpectedly vs existing.
    Tune thresholds per-table in a real system.
    """
    if incoming_rows < 0:
        raise AssertionError("Incoming rows negative")
    if existing_rows and incoming_rows > max(10_000, existing_rows * 5):
        raise AssertionError(f"Row explosion: {incoming_rows} vs {existing_rows}")
