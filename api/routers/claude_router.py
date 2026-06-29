"""Claude RAG compliance query router."""

from fastapi import APIRouter, Body
from services.claude_rag import query_compliance_agent
from services.postgres_client import insert_audit_event

router = APIRouter(prefix="/compliance", tags=["Compliance AI"])

@router.post("/query")
async def compliance_query(
    question: str = Body(..., embed=True),
    symbol:   str = Body(None, embed=True)
):
    """
    Ask Claude a natural language compliance question grounded in live data.

    Example questions:
    - "Are there any suspicious BTC trades in the last 30 minutes?"
    - "What is the current risk level across all monitored symbols?"
    - "Show me any FINTRAC-reportable events today"
    - "Is the current ETH VWAP deviation significant enough to escalate?"
    """
    # Log every query to FINTRAC audit trail
    insert_audit_event(
        event_type="AI_COMPLIANCE_QUERY",
        actor="compliance-analyst",
        action=f"Query: {question[:200]}",
        metadata={"symbol": symbol, "question": question}
    )

    result = query_compliance_agent(question=question, symbol=symbol)

    # Log the response too — full audit trail
    insert_audit_event(
        event_type="AI_COMPLIANCE_RESPONSE",
        actor="claude-sonnet",
        action=f"Response generated ({result.get('usage', {}).get('output_tokens', 0)} tokens)",
        metadata={"context_used": result.get("context_used")}
    )

    return {
        "question": question,
        "symbol":   symbol,
        **result
    }
