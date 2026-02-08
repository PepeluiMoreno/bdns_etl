"""
ETL Admin Backend - API REST para administración de procesos ETL

Backend separado para gestión de procesos ETL (seeding y sync).
Incluye autenticación JWT compartida con bdns-search-backend.
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from etl_admin.api.auth import router as auth_router
from etl_admin.api.etl_router import router as etl_router


# Crear app FastAPI
app = FastAPI(
    title="BDNS ETL Admin API",
    description="API REST para administración de procesos ETL (seeding y sync)",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# CORS - Permitir acceso desde frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3001",  # ETL Admin Frontend (dev)
        "http://localhost:3000",  # BDNS Search Frontend (dev)
        # Añadir URLs de producción aquí
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Routers
app.include_router(auth_router, prefix="/api/auth", tags=["Auth"])
app.include_router(etl_router, prefix="/api/etl", tags=["ETL"])


@app.get("/")
async def root():
    return {
        "service": "BDNS ETL Admin API",
        "version": "1.0.0",
        "docs": "/docs",
        "health": "/health"
    }


@app.get("/health")
async def health():
    """Health check endpoint."""
    return {"status": "ok", "service": "etl-admin-backend"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8001,
        reload=True
    )
