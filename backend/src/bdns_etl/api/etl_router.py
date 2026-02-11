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
            "'grandes_beneficiarios' o 'all'"
        )
    )
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
    # Si no se especifica año, usar el año actual
    if not year:
        from datetime import datetime
        year = datetime.now().year

    # Verificar dependencias
    system_status = etl_service.get_system_status()
    catalogos_info = system_status["catalogos"]
    total_catalogos = len(catalogos_info["detalle"])
    catalogos_ok = sum(1 for c in catalogos_info["detalle"] if c["estado"] == "ok")
    total_registros_cat = sum(c["registros"] for c in catalogos_info["detalle"])

    convocatorias_seeded = etl_service.check_convocatorias_seeded(year)

    # Consultar última ejecución exitosa de cada entidad (devuelve dict con stats)
    ultima_cat = etl_service.get_last_successful_execution("catalogos")

    # catalogos_seeded: tablas pobladas O ejecución exitosa de catálogos
    catalogos_seeded = catalogos_info["inicializados"] or (ultima_cat is not None)

    catalogos_reason = None
    if not catalogos_seeded:
        faltantes = [c["nombre"] for c in catalogos_info["detalle"] if c["estado"] != "ok"]
        catalogos_reason = f"Debe poblar primero los catálogos ({', '.join(faltantes)})"

    convocatorias_reason = None if convocatorias_seeded else "Debe poblar antes las convocatorias de ese año"
    ultima_conv = etl_service.get_last_successful_execution("convocatorias", year)
    ultima_conc = etl_service.get_last_successful_execution("concesiones", year)
    ultima_min = etl_service.get_last_successful_execution("minimis", year)
    ultima_ay = etl_service.get_last_successful_execution("ayudas_estado", year)
    ultima_pp = etl_service.get_last_successful_execution("partidos_politicos", year)

    def _fmt(exec_data):
        """Extrae finished_at como ISO string de los datos de ejecución."""
        if not exec_data:
            return None
        dt = exec_data.get("finished_at")
        return dt.isoformat() if dt else None

    def _cambios(exec_data):
        """Extrae cambios_pendientes de los datos de ejecución."""
        if not exec_data:
            return {"nuevos": 0, "actualizados": 0, "borrados": 0}
        return {
            "nuevos": exec_data.get("records_inserted", 0),
            "actualizados": exec_data.get("records_updated", 0),
            "borrados": 0,
        }

    # Para catálogos: progreso basado en ejecución completada, no solo en conteo de tablas
    catalogos_progreso = round(catalogos_ok / total_catalogos * 100) if total_catalogos else 0
    if ultima_cat:
        # Si hubo ejecución exitosa, el progreso mínimo es el basado en tablas,
        # pero el estado se considera completo
        catalogos_progreso = max(catalogos_progreso, 100)

    # Definición de entidades
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
            "seed_blocked_reason": None
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
            "seed_blocked_reason": catalogos_reason
        },
        {
            "id": "concesiones",
            "nombre": "Concesiones",
            "descripcion": "Concesiones genéricas de ayudas",
            "ultima_sync": _fmt(ultima_conc),
            "estado": "complete" if ultima_conc else "pending",
            "progreso": 100 if ultima_conc else 0,
            "cambios_pendientes": _cambios(ultima_conc),
            "can_seed": convocatorias_seeded,
            "seed_blocked_reason": convocatorias_reason
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
            "seed_blocked_reason": convocatorias_reason
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
            "seed_blocked_reason": convocatorias_reason
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
            "seed_blocked_reason": convocatorias_reason
        }
    ]

    # Rellenar campos por defecto
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

    Body:
        - entity_id: ID de la entidad (convocatorias, concesiones, etc.)
        - year: Año a procesar (opcional)

    El proceso se ejecuta en background y retorna inmediatamente.
    """
    entity_id = request.get("entity_id")
    year = request.get("year")

    if not entity_id:
        raise HTTPException(status_code=400, detail="entity_id es requerido")

    try:
        # Iniciar seeding para la entidad específica
        result = await etl_service.start_seeding(
            year=year or 2024,
            entity=entity_id,
            batch_size=5000
        )

        # Retornar estado actualizado de la entidad
        return {
            "id": entity_id,
            "nombre": entity_id.capitalize(),
            "estado": "seeding",
            "progreso": 0,
            "execution_id": result.get("execution_id"),
            "message": f"Seeding iniciado para {entity_id}"
        }
    except ValueError as e:
        # Errores de validación (ej: dependencias no cumplidas)
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
    except ValueError as e:
        # Errores de validación (ej: dependencias no cumplidas)
        raise HTTPException(status_code=400, detail=str(e))
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

    Envía actualizaciones cada 2 segundos con:
    - Estadísticas generales
    - Ejecuciones recientes
    - Procesos activos con progreso detallado

    NOTA: Este endpoint NO requiere autenticación JWT para simplificar.
    En producción, considerar implementar autenticación WS.
    """
    await manager.connect(websocket)
    try:
        while True:
            # Obtener datos actualizados
            stats = etl_service.get_statistics_summary()
            recent = etl_service.list_recent_executions(limit=10)
            active = etl_service.list_active_executions()

            # Enviar actualización completa
            await websocket.send_json({
                "type": "stats_update",
                "data": {
                    "statistics": stats,
                    "recent_executions": recent,
                    "active_processes": active
                },
                "timestamp": asyncio.get_event_loop().time()
            })

            # Enviar actualizaciones individuales de procesos activos
            for process in active:
                await websocket.send_json({
                    "type": "process_update",
                    "data": process,
                    "timestamp": asyncio.get_event_loop().time()
                })

            await asyncio.sleep(2)  # Actualizar cada 2 segundos

    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception as e:
        import traceback
        error_trace = traceback.format_exc()
        print(f"WebSocket error: {e}")
        print(f"Traceback: {error_trace}")
        manager.disconnect(websocket)
