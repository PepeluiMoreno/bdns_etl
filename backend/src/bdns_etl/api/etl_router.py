"""
bdns_etl/api/etl_router.py

Router FastAPI para administración de procesos ETL.
Endpoints REST para lanzar procesos, monitorear progreso, consultar estadísticas.

Todos los endpoints requieren autenticación JWT.
Los endpoints de modificación requieren rol de admin.
"""

import asyncio
from typing import List, Optional
from uuid import UUID

from fastapi import APIRouter, HTTPException, WebSocket, WebSocketDisconnect, Query, Depends
from pydantic import BaseModel, Field

from bdns_etl.services.etl_service import etl_service
from bdns_etl.api.auth import get_current_user, require_admin
from bdns_core.auth import UserInToken


# ==========================================
# SCHEMAS PYDANTIC
# ==========================================

class StartSeedingRequest(BaseModel):
    """Request para iniciar seeding."""
    year: int = Field(..., ge=2015, le=2030, description="Año a procesar")
    entity: str = Field(
        default="all",
        description=(
            "Entidad: 'convocatorias', 'concesiones', 'catalogos', "
            "'minimis', 'ayudas_estado', 'partidos_politicos', "
            "'grandes_beneficiarios', 'all_concesiones' o 'all'"
        )
    )
    batch_size: int = Field(default=5000, ge=100, le=10000, description="Tamaño de batch")
    skip_extract: bool = Field(default=False, description="Saltar extracción si JSONL existen")
    skip_transform: bool = Field(default=False, description="Saltar transformación si JSONL transformado existe")


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
    execution_type: str
    entity: str
    year: Optional[int]
    status: str
    started_at: Optional[str]
    finished_at: Optional[str]
    progress: dict
    stats: dict
    error: Optional[str]


# ==========================================
# ROUTER
# ==========================================

router = APIRouter()


# ==========================================
# ESTADO DEL SISTEMA (sin auth - health check)
# ==========================================

@router.get("/system-status")
async def get_system_status():
    """
    Estado general del sistema: backend, BD y catálogos.

    NO requiere autenticación (es un health-check extendido).
    """
    return etl_service.get_system_status()


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
    status: Optional[str] = Query(default=None, description="Filtrar por estado: 'running', 'completed', 'failed'"),
    current_user: UserInToken = Depends(get_current_user)
):
    """
    Lista las ejecuciones más recientes o activas.

    Requiere autenticación.
    """
    if status == "running":
        return etl_service.list_active_executions()
    else:
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


@router.get("/entities/status")
async def get_entities_status(
    year: Optional[int] = Query(default=None, description="Año para filtrar entidades"),
    current_user: UserInToken = Depends(get_current_user)
):
    """
    Obtiene el estado de todas las entidades para un año.

    Requiere autenticación.
    """
    if not year:
        from datetime import datetime
        year = datetime.now().year

    system_status = etl_service.get_system_status()
    catalogos_info = system_status["catalogos"]
    total_catalogos = len(catalogos_info["detalle"])
    catalogos_ok = sum(1 for c in catalogos_info["detalle"] if c["estado"] == "ok")
    total_registros_cat = sum(c["registros"] for c in catalogos_info["detalle"])

    convocatorias_seeded = etl_service.check_convocatorias_seeded(year)

    ultima_cat = etl_service.get_last_successful_execution("catalogos")
    catalogos_seeded = catalogos_info["inicializados"] or (ultima_cat is not None)

    catalogos_reason = None
    if not catalogos_seeded:
        faltantes = [c["nombre"] for c in catalogos_info["detalle"] if c["estado"] != "ok"]
        catalogos_reason = f"Debe poblar primero los catálogos ({', '.join(faltantes)})"

    convocatorias_reason = None if convocatorias_seeded else "Debe poblar antes las convocatorias de ese año"
    ultima_conv = etl_service.get_last_successful_execution("convocatorias", year)
    ultima_conc = etl_service.get_last_successful_execution("all_concesiones", year)
    ultima_min = etl_service.get_last_successful_execution("minimis", year)
    ultima_ay = etl_service.get_last_successful_execution("ayudas_estado", year)
    ultima_pp = etl_service.get_last_successful_execution("partidos_politicos", year)

    int_cat = etl_service.get_last_interrupted_execution("catalogos")
    int_conv = etl_service.get_last_interrupted_execution("convocatorias", year)
    int_conc = etl_service.get_last_interrupted_execution("all_concesiones", year)
    int_min = etl_service.get_last_interrupted_execution("minimis", year)
    int_ay = etl_service.get_last_interrupted_execution("ayudas_estado", year)
    int_pp = etl_service.get_last_interrupted_execution("partidos_politicos", year)

    def _fmt(exec_data):
        if not exec_data:
            return None
        dt = exec_data.get("finished_at")
        return dt.isoformat() if dt else None

    def _cambios(exec_data):
        if not exec_data:
            return {"nuevos": 0, "actualizados": 0, "borrados": 0}
        return {
            "nuevos": exec_data.get("records_inserted", 0),
            "actualizados": exec_data.get("records_updated", 0),
            "borrados": 0,
        }

    catalogos_progreso = round(catalogos_ok / total_catalogos * 100) if total_catalogos else 0
    if ultima_cat:
        catalogos_progreso = max(catalogos_progreso, 100)

    entity_defs = [
        {
            "id": "catalogos",
            "nombre": "Catálogos de la aplicación",
            "descripcion": "Tablas de referencia necesarias para la integridad de datos",
            "progreso": catalogos_progreso,
            "total_registros": total_registros_cat,
            "registros_procesados": total_registros_cat,
            "ultima_sync": _fmt(ultima_cat),
            "estado": "complete" if ultima_cat else "pending",
            "cambios_pendientes": _cambios(ultima_cat),
            "atemporal": True,
            "can_seed": True,
            "seed_blocked_reason": None,
            "interrupted_execution": int_cat,
        },
        {
            "id": "convocatorias",
            "nombre": "Convocatorias",
            "descripcion": "Convocatorias de ayudas y subvenciones",
            "ultima_sync": _fmt(ultima_conv),
            "estado": "complete" if ultima_conv else "pending",
            "progreso": 100 if ultima_conv else 0,
            "cambios_pendientes": _cambios(ultima_conv),
            "can_seed": catalogos_seeded,
            "seed_blocked_reason": catalogos_reason,
            "interrupted_execution": int_conv,
        },
        {
            "id": "all_concesiones",
            "nombre": "Concesiones (JSONL)",
            "descripcion": "Concesiones de 4 fuentes (ordinarias, minimis, ayudas estado, partidos políticos) vía JSONL + COPY nativo",
            "ultima_sync": _fmt(ultima_conc),
            "estado": "complete" if ultima_conc else "pending",
            "progreso": 100 if ultima_conc else 0,
            "cambios_pendientes": _cambios(ultima_conc),
            "can_seed": convocatorias_seeded,
            "seed_blocked_reason": convocatorias_reason,
            "interrupted_execution": int_conc,
        },
        {
            "id": "minimis",
            "nombre": "De Minimis",
            "descripcion": "Ayudas de régimen de minimis",
            "ultima_sync": _fmt(ultima_min),
            "estado": "complete" if ultima_min else "pending",
            "progreso": 100 if ultima_min else 0,
            "cambios_pendientes": _cambios(ultima_min),
            "can_seed": convocatorias_seeded,
            "seed_blocked_reason": convocatorias_reason,
            "interrupted_execution": int_min,
        },
        {
            "id": "ayudas_estado",
            "nombre": "Ayudas de Estado",
            "descripcion": "Ayudas de régimen de estado",
            "ultima_sync": _fmt(ultima_ay),
            "estado": "complete" if ultima_ay else "pending",
            "progreso": 100 if ultima_ay else 0,
            "cambios_pendientes": _cambios(ultima_ay),
            "can_seed": convocatorias_seeded,
            "seed_blocked_reason": convocatorias_reason,
            "interrupted_execution": int_ay,
        },
        {
            "id": "partidos_politicos",
            "nombre": "Partidos Políticos",
            "descripcion": "Concesiones a partidos políticos",
            "ultima_sync": _fmt(ultima_pp),
            "estado": "complete" if ultima_pp else "pending",
            "progreso": 100 if ultima_pp else 0,
            "cambios_pendientes": _cambios(ultima_pp),
            "can_seed": convocatorias_seeded,
            "seed_blocked_reason": convocatorias_reason,
            "interrupted_execution": int_pp,
        }
    ]

    defaults = {
        "total_registros": 0,
        "registros_procesados": 0,
        "cambios_pendientes": {"nuevos": 0, "actualizados": 0, "borrados": 0},
        "auto_resync_eligible": False,
        "atemporal": False,
    }
    for e in entity_defs:
        for k, v in defaults.items():
            e.setdefault(k, v)

    return entity_defs


@router.get("/concesiones/jsonl-status/{year}")
async def get_concesiones_jsonl_status(
    year: int,
    current_user: UserInToken = Depends(get_current_user)
):
    """
    Obtiene estado de archivos JSONL para decidir qué fases ejecutar.
    """
    return etl_service.check_concesiones_jsonl_status(year)


# ==========================================
# ENDPOINTS DE MODIFICACIÓN (requieren admin)
# ==========================================

@router.post("/entities/seed")
async def seed_entity(
    request: dict,
    current_user: UserInToken = Depends(require_admin)
):
    """
    Inicia proceso de seeding para una entidad específica.

    **Requiere rol de admin.**
    """
    entity_id = request.get("entity_id")
    year = request.get("year")
    replacing_execution_id = request.get("replacing_execution_id")
    skip_extract = request.get("skip_extract", False)
    skip_transform = request.get("skip_transform", False)

    if not entity_id:
        raise HTTPException(status_code=400, detail="entity_id es requerido")

    replacing_uuid = UUID(replacing_execution_id) if replacing_execution_id else None

    try:
        if entity_id == "all_concesiones":
            result = await etl_service.start_concesiones_jsonl_pipeline(
                year=year or 2024,
                skip_extract=skip_extract,
                skip_transform=skip_transform,
                replacing_execution_id=replacing_uuid
            )
        else:
            result = await etl_service.start_seeding(
                year=year or 2024,
                entity=entity_id,
                batch_size=5000,
                replacing_execution_id=replacing_uuid
            )

        return {
            "id": entity_id,
            "nombre": entity_id.capitalize(),
            "estado": "seeding",
            "progreso": 0,
            "execution_id": result.get("execution_id"),
            "message": f"Seeding iniciado para {entity_id}"
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/seeding/start", response_model=ExecutionResponse)
async def start_seeding(
    request: StartSeedingRequest,
    current_user: UserInToken = Depends(require_admin)
):
    """
    Inicia proceso de seeding (carga inicial) para un año.

    **Requiere rol de admin.**
    """
    try:
        if request.entity == "all_concesiones":
            result = await etl_service.start_concesiones_jsonl_pipeline(
                year=request.year,
                skip_extract=request.skip_extract,
                skip_transform=request.skip_transform
            )
        else:
            result = await etl_service.start_seeding(
                year=request.year,
                entity=request.entity,
                batch_size=request.batch_size
            )
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/coverage")
async def get_coverage(
    current_user: UserInToken = Depends(get_current_user)
):
    """
    Obtiene cobertura de datos por entidad (registros cargados por año).
    """
    try:
        entities = etl_service.get_coverage()
        return {"entities": entities}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/alerts")
async def get_alerts(
    current_user: UserInToken = Depends(get_current_user)
):
    """
    Obtiene alertas activas del sistema ETL.
    """
    try:
        alerts = etl_service.get_alerts()
        return alerts
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/retry-failed")
async def retry_failed(
    current_user: UserInToken = Depends(require_admin)
):
    """
    Reintenta todas las ejecuciones fallidas más recientes.

    **Requiere rol de admin.**
    """
    try:
        result = await etl_service.retry_failed_executions()
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


@router.delete("/execution/{execution_id}")
async def delete_execution(
    execution_id: UUID,
    current_user: UserInToken = Depends(require_admin)
):
    """
    Elimina un registro de ejecución del historial.

    **Requiere rol de admin.**
    """
    result = etl_service.delete_execution(execution_id)
    if not result["success"]:
        raise HTTPException(status_code=400, detail=result.get("error", "Unknown error"))
    return result


@router.post("/entities/{entity}/clean-temp")
async def clean_temp_files(
    entity: str,
    year: Optional[int] = Query(None),
    current_user: UserInToken = Depends(require_admin)
):
    """
    Elimina archivos temporales de una entidad/año.

    **Requiere rol de admin.**
    """
    result = etl_service.clean_temp_files(entity, year)
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
                pass


manager = ConnectionManager()


@router.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """
    WebSocket para recibir actualizaciones de progreso en tiempo real.
    """
    await manager.connect(websocket)
    try:
        while True:
            stats = etl_service.get_statistics_summary()
            recent = etl_service.list_recent_executions(limit=10)
            active = etl_service.list_active_executions()

            await websocket.send_json({
                "type": "stats_update",
                "data": {
                    "statistics": stats,
                    "recent_executions": recent,
                    "active_processes": active
                },
                "timestamp": asyncio.get_event_loop().time()
            })

            for process in active:
                await websocket.send_json({
                    "type": "process_update",
                    "data": process,
                    "timestamp": asyncio.get_event_loop().time()
                })

            await asyncio.sleep(2)

    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception as e:
        import traceback
        traceback.print_exc()
        manager.disconnect(websocket)