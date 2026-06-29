"""
Crypto Risk + AML Compliance Dashboard API
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routers import risk, compliance
from config import get_allowed_origins
import logging

logging.basicConfig(level=logging.INFO)

app = FastAPI(
    title="Crypto Risk + AML Compliance API",
    description="Real-time market risk and AML surveillance platform — Scotia Bank / Coinbase FDE",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=get_allowed_origins(),
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(risk.router)
app.include_router(compliance.router)

@app.get("/health")
async def health():
    return {"status": "healthy", "service": "crypto-risk-aml-api"}
