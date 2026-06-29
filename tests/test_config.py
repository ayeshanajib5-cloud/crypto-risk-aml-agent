from api.config import get_allowed_origins


def test_allowed_origins_are_loaded_from_environment(monkeypatch):
    monkeypatch.setenv(
        "ALLOWED_ORIGINS",
        "https://dashboard.example.com, http://localhost:3000 ",
    )

    assert get_allowed_origins() == [
        "https://dashboard.example.com",
        "http://localhost:3000",
    ]


def test_allowed_origins_falls_back_to_local_development(monkeypatch):
    monkeypatch.delenv("ALLOWED_ORIGINS", raising=False)

    assert "http://localhost:3000" in get_allowed_origins()
