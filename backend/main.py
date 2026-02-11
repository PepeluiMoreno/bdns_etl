"""
ETL Admin Backend - API REST para administración de procesos ETL.

Backend separado para gestión de procesos ETL (seeding y sync).
Incluye autenticación JWT compartida con bdns_core.

Usa configuración centralizada de bdns_core.
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from bdns_etl.api.auth import router as auth_router
from bdns_etl.api.etl_router import router as etl_router
from bdns_core.config import get_etl_settings


# Cargar settings
settings = get_etl_settings()

# Crear app FastAPI
app = FastAPI(
    title="BDNS ETL Admin API",
    description="API REST para administración de procesos ETL (seeding y sync)",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    debug=settings.DEBUG,
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.get_cors_origins(),
    allow_credentials=settings.CORS_ALLOW_CREDENTIALS,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Routers
app.include_router(auth_router, prefix="/api/auth", tags=["Auth"])
app.include_router(etl_router, prefix="/api/etl", tags=["ETL"])


@app.get("/")
async def root():
    """Root endpoint con información del servicio."""
    return {
        "service": "BDNS ETL Admin API",
        "version": "1.0.0",
        "environment": settings.ENVIRONMENT,
        "docs": "/docs",
        "health": "/health"
    }


@app.get("/health")
async def health():
    """Health check endpoint."""
    return {
        "status": "ok",
        "service": "etl-admin-backend",
        "version": "1.0.0",
        "environment": settings.ENVIRONMENT,
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host=settings.HOST,
        port=settings.PORT,
        reload=settings.DEBUG,
        workers=1 if settings.DEBUG else settings.WORKERS,
    )
