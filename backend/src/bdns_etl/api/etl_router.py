"""
Router FastAPI para administración de procesos ETL.

Endpoints REST para:
- Lanzar procesos de seeding y sync
- Monitorear progreso
- Consultar estadísticas
- WebSocket para actualizaciones en tiempo real

Todos los endpoints requieren autenticación JWT.
Los endpoints de modificación requieren rol de admin.
"""
import asyncio
from typing import List, Optional
from uuid import UUID

from fastapi import APIRouter, HTTPException, WebSocket, WebSocketDisconnect, Query, Depends
from pydantic import BaseModel, Field

from etl_admin.services.etl_service import etl_service
from etl_admin.api.auth import get_current_user, require_admin
from bdns_core.auth import UserInToken


# ==========================================
# SCHEMAS PYDANTIC
# ==========================================

class StartSeedingRequest(BaseModel):
    """Request para iniciar seeding."""
    year: int = Field(..., ge=2015, le=2030, description="Año a procesar")
    entity: str = Field(default="all", description="Entidad: 'convocatorias', 'concesiones' o 'all'")
    batch_size: int = Field(default=5000, ge=100, le=10000, description="Tamaño de batch")


class StartSyncRequest(BaseModel):
    """Request para iniciar sincronización."""
    year: int = Field(..., ge=2015, le=2030, description="Año a sincronizar")
    entity: str = Field(default="all", description="Entidad: 'convocatorias', 'concesiones' o 'all'")
    incremental: bool = Field(default=True, description="Sincronización incremental")
    days_back: int = Field(default=7, ge=1, le=365, description="Días hacia atrás para sync incremental")


class ExecutionResponse(BaseModel):
    """Response de ejecución iniciada."""
    execution_id: str
    status: str
    year: int
    entity: str
    started_at: str


class ExecutionStatusResponse(BaseModel):
    """Response con estado detallado de ejecución."""
    execution_id: str
    process_type: str
    entity: str
    year: Optional[int]
    status: str
    started_at: Optional[str]
    finished_at: Optional[str]
    progress: dict
    stats: dict
    config: dict
    error: Optional[str]


# ==========================================
# ROUTER
# ==========================================

router = APIRouter()


# ==========================================
# ENDPOINTS DE CONSULTA (requieren auth)
# ==========================================

@router.get("/execution/{execution_id}", response_model=ExecutionStatusResponse)
async def get_execution_status(
    execution_id: UUID,
    current_user: UserInToken = Depends(get_current_user)
):
    """
    Obtiene el estado actual de una ejecución.

    Requiere autenticación.
    """
    status = etl_service.get_execution_status(execution_id)
    if not status:
        raise HTTPException(status_code=404, detail="Execution not found")
    return status


@router.get("/executions")
async def list_executions(
    limit: int = Query(default=20, ge=1, le=100),
    current_user: UserInToken = Depends(get_current_user)
):
    """
    Lista las ejecuciones más recientes.

    Requiere autenticación.
    """
    return etl_service.list_recent_executions(limit=limit)


@router.get("/statistics")
async def get_statistics(current_user: UserInToken = Depends(get_current_user)):
    """
    Obtiene estadísticas generales de ETL.

    Requiere autenticación.
    """
    return etl_service.get_statistics_summary()


@router.get("/sync-control")
async def get_sync_control(current_user: UserInToken = Depends(get_current_user)):
    """
    Obtiene el estado de sincronización por entidad.

    Requiere autenticación.
    """
    return etl_service.get_sync_control_status()


# ==========================================
# ENDPOINTS DE MODIFICACIÓN (requieren admin)
# ==========================================

@router.post("/seeding/start", response_model=ExecutionResponse)
async def start_seeding(
    request: StartSeedingRequest,
    current_user: UserInToken = Depends(require_admin)
):
    """
    Inicia proceso de seeding (carga inicial) para un año.

    **Requiere rol de admin.**

    El proceso se ejecuta en background y retorna inmediatamente.
    Usa /execution/{execution_id} o WebSocket para monitorear progreso.
    """
    try:
        result = await etl_service.start_seeding(
            year=request.year,
            entity=request.entity,
            batch_size=request.batch_size
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/sync/start", response_model=ExecutionResponse)
async def start_sync(
    request: StartSyncRequest,
    current_user: UserInToken = Depends(require_admin)
):
    """
    Inicia proceso de sincronización (actualización incremental).

    **Requiere rol de admin.**

    El proceso se ejecuta en background y retorna inmediatamente.
    Usa /execution/{execution_id} o WebSocket para monitorear progreso.
    """
    try:
        result = await etl_service.start_sync(
            year=request.year,
            entity=request.entity,
            incremental=request.incremental,
            days_back=request.days_back
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/execution/{execution_id}/stop")
async def stop_execution(
    execution_id: UUID,
    current_user: UserInToken = Depends(require_admin)
):
    """
    Detiene una ejecución en curso.

    **Requiere rol de admin.**
    """
    result = await etl_service.stop_execution(execution_id)
    if not result["success"]:
        raise HTTPException(status_code=404, detail=result.get("error", "Unknown error"))
    return result


# ==========================================
# WEBSOCKET para actualizaciones en tiempo real
# ==========================================

class ConnectionManager:
    """Gestiona conexiones WebSocket para streaming de progreso."""

    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def broadcast(self, message: dict):
        """Envía mensaje a todos los clientes conectados."""
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except:
                pass  # Cliente desconectado, ignorar


manager = ConnectionManager()


@router.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """
    WebSocket para recibir actualizaciones de progreso en tiempo real.

    Envía actualizaciones cada segundo con el estado de todas las ejecuciones activas.

    NOTA: Este endpoint NO requiere autenticación JWT para simplificar.
    En producción, considerar implementar autenticación WS.
    """
    await manager.connect(websocket)
    try:
        while True:
            # Enviar estadísticas cada segundo
            stats = etl_service.get_statistics_summary()
            recent = etl_service.list_recent_executions(limit=5)

            await websocket.send_json({
                "type": "stats_update",
                "data": {
                    "statistics": stats,
                    "recent_executions": recent
                },
                "timestamp": asyncio.get_event_loop().time()
            })

            await asyncio.sleep(1)

    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception as e:
        manager.disconnect(websocket)
        print(f"WebSocket error: {e}")
