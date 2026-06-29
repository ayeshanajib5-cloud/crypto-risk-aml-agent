from fastapi.testclient import TestClient

from api.main import app


def test_health_endpoint_returns_service_status():
    client = TestClient(app)

    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {
        "status": "healthy",
        "service": "crypto-risk-aml-api",
    }


def test_openapi_schema_exposes_risk_and_compliance_routes():
    client = TestClient(app)

    schema = client.get("/openapi.json").json()

    assert "/risk/dashboard" in schema["paths"]
    assert "/risk/signals" in schema["paths"]
    assert "/compliance/alerts" in schema["paths"]
    assert "/compliance/audit" in schema["paths"]
