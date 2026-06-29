"""Compliance and FINTRAC router."""

from fastapi import APIRouter, Query
from services.postgres_client import fetch_aml_alerts, fetch_audit_log, insert_audit_event

router = APIRouter(prefix="/compliance", tags=["Compliance"])

@router.get("/alerts")
async def get_alerts(
    status: str = Query(None, description="OPEN, UNDER_REVIEW, ESCALATED, STR_FILED"),
    limit:  int = Query(50, le=200)
):
    """AML alerts for compliance officer review."""
    insert_audit_event(
        "ALERT_QUERY", "api", f"Queried alerts status={status} limit={limit}"
    )
    return fetch_aml_alerts(status=status, limit=limit)

@router.get("/audit")
async def get_audit_log(limit: int = Query(100, le=1000)):
    """FINTRAC immutable audit trail."""
    return fetch_audit_log(limit=limit)

@router.get("/summary")
async def compliance_summary():
    """High-level compliance summary for executive dashboard."""
    alerts = fetch_aml_alerts(limit=1000)
    return {
        "total_alerts":      len(alerts),
        "open":              sum(1 for a in alerts if a.get("status") == "OPEN"),
        "under_review":      sum(1 for a in alerts if a.get("status") == "UNDER_REVIEW"),
        "fintrac_reportable": sum(1 for a in alerts if a.get("fintrac_reportable")),
        "str_filed":         sum(1 for a in alerts if a.get("status") == "STR_FILED"),
    }
