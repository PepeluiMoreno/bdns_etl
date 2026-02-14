"""
Servicio para gestión y monitoreo de procesos ETL.

Proporciona funcionalidades para:
- Lanzar procesos ETL (seeding y sync)
- Monitorear progreso en tiempo real
- Consultar estadísticas de ejecuciones
- Gestionar jobs y executions
"""
import asyncio
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional
from uuid import UUID

from sqlalchemy import select, func, desc, text
from sqlalchemy.orm import Session

from bdns_core.db.session import get_session
from bdns_core.db.etl_models import EtlJob, EtlExecution, SyncControl


class ETLService:
    """Servicio para gestión de procesos ETL."""

    def __init__(self):
        # Desde: bdns_etl/backend/src/bdns_etl/services/etl_service.py
        # Hasta: bdns_etl/etl_scripts/
        self.etl_root = Path(__file__).parent.parent.parent.parent.parent / "etl_scripts"
        self.seeding_dir = self.etl_root.parent / "seeding"
        self.active_processes: Dict[UUID, asyncio.subprocess.Process] = {}
        self._cleanup_stale_executions()

    def _cleanup_stale_executions(self):
        """Marca como 'interrupted' las ejecuciones que quedaron en 'running' tras un reinicio."""
        try:
            with get_session() as session:
                stale = session.execute(
                    select(EtlExecution).where(EtlExecution.status == "running")
                ).scalars().all()
                if stale:
                    for ex in stale:
                        ex.status = "interrupted"
                        ex.finished_at = datetime.utcnow()
                        ex.error_message = "Proceso interrumpido por reinicio del servidor"
                    session.commit()
                    print(f"[ETLService] {len(stale)} ejecución(es) huérfana(s) marcadas como interrupted")
        except Exception as e:
            print(f"[ETLService] Error limpiando ejecuciones huérfanas: {e}")

    # ==========================================
    # ESTADO DEL SISTEMA
    # ==========================================

    CATALOG_TABLES = [
        {"tabla": "organo", "nombre": "Órganos convocantes"},
        {"tabla": "region", "nombre": "Regiones"},
        {"tabla": "instrumento", "nombre": "Instrumentos"},
        {"tabla": "tipo_beneficiario", "nombre": "Tipos de beneficiario"},
        {"tabla": "sector_producto", "nombre": "Sectores de producto"},
        {"tabla": "finalidad", "nombre": "Finalidades"},
        {"tabla": "objetivo", "nombre": "Objetivos"},
        {"tabla": "reglamento", "nombre": "Reglamentos"},
    ]

    def get_system_status(self) -> Dict[str, Any]:
        """Obtiene el estado general del sistema: BD y catálogos."""
        result = {
            "backend": "ok",
            "database": False,
            "catalogos": {
                "inicializados": False,
                "detalle": []
            }
        }

        try:
            with get_session() as session:
                session.execute(text("SELECT 1"))
            result["database"] = True
        except Exception:
            return result

        todos_ok = True
        for cat in self.CATALOG_TABLES:
            tabla = cat["tabla"]
            try:
                with get_session() as session:
                    count = session.execute(
                        text(f"SELECT COUNT(*) FROM bdns.{tabla}")
                    ).scalar()
                estado = "ok" if count and count > 0 else "missing"
            except Exception:
                count = 0
                estado = "missing"

            if estado != "ok":
                todos_ok = False

            result["catalogos"]["detalle"].append({
                "tabla": tabla,
                "nombre": cat["nombre"],
                "registros": count or 0,
                "estado": estado
            })

        result["catalogos"]["inicializados"] = todos_ok
        return result

    # ==========================================
    # VALIDACIONES
    # ==========================================

    def get_last_successful_execution(self, entity: str, year: Optional[int] = None) -> Optional[Dict[str, Any]]:
        """Obtiene datos de la última ejecución exitosa para una entidad."""
        try:
            with get_session() as session:
                query = (
                    select(EtlExecution)
                    .where(EtlExecution.entity == entity)
                    .where(EtlExecution.status == "completed")
                )
                if year is not None:
                    query = query.where(EtlExecution.year == year)
                query = query.order_by(desc(EtlExecution.finished_at)).limit(1)
                execution = session.execute(query).scalar_one_or_none()
                if not execution:
                    return None
                return {
                    "finished_at": execution.finished_at,
                    "records_processed": execution.records_processed or 0,
                    "records_inserted": execution.records_inserted or 0,
                    "records_updated": execution.records_updated or 0,
                    "records_errors": execution.records_errors or 0,
                }
        except Exception:
            return None

    def get_last_interrupted_execution(self, entity: str, year: Optional[int] = None) -> Optional[Dict[str, Any]]:
        """Obtiene la última ejecución interrumpida o fallida (candidata a reinicio)."""
        try:
            with get_session() as session:
                query = (
                    select(EtlExecution)
                    .where(EtlExecution.entity == entity)
                    .where(EtlExecution.status.in_(["interrupted", "failed"]))
                )
                if year is not None:
                    query = query.where(EtlExecution.year == year)
                query = query.order_by(desc(EtlExecution.finished_at)).limit(1)
                execution = session.execute(query).scalar_one_or_none()
                if not execution:
                    return None
                return {
                    "execution_id": str(execution.id),
                    "status": execution.status,
                    "finished_at": execution.finished_at,
                    "error_message": execution.error_message,
                    "progress": execution.progress_percentage or 0,
                }
        except Exception:
            return None

    def check_catalogos_seeded(self) -> dict:
        """Verifica que los catálogos necesarios estén poblados."""
        required_catalogs = ["organo", "reglamento"]
        missing = {}
        for catalog in required_catalogs:
            try:
                with get_session() as session:
                    count = session.execute(
                        text(f"SELECT COUNT(*) FROM bdns.{catalog}")
                    ).scalar()
                    if not count:
                        missing[catalog] = 0
            except Exception:
                missing[catalog] = 0
        return missing

    def check_convocatorias_seeded(self, year: int) -> bool:
        """Verifica si existen convocatorias pobladas para un año dado."""
        with get_session() as session:
            stmt = select(EtlExecution).where(
                EtlExecution.execution_type == "seeding",
                EtlExecution.entity == "convocatorias",
                EtlExecution.year == year,
                EtlExecution.status == "completed"
            )
            result = session.execute(stmt).first()
            return result is not None

    def check_concesiones_jsonl_status(self, year: int) -> Dict[str, Any]:
        """Verifica estado de archivos JSONL para decidir qué fases ejecutar."""
        jsonl_dir = self.seeding_dir / "concesiones" / "data" / "jsonl"
        transformed_dir = jsonl_dir / "transformed"

        sources = {
            "ordinarias": jsonl_dir / f"concesiones_ordinarias_{year}.jsonl",
            "minimis": jsonl_dir / f"concesiones_minimis_{year}.jsonl",
            "ayudas_estado": jsonl_dir / f"concesiones_ayudasestado_{year}.jsonl",
            "partidos_politicos": jsonl_dir / f"concesiones_partidospoliticos_{year}.jsonl",
        }

        transformed = transformed_dir / f"concesiones_{year}.jsonl"

        result = {
            "year": year,
            "extract": {},
            "transformed": {
                "exists": transformed.exists(),
                "path": str(transformed),
            },
            "can_skip_extract": True,
            "can_skip_transform": transformed.exists(),
        }

        for name, path in sources.items():
            exists = path.exists()
            size = path.stat().st_size if exists else 0
            result["extract"][name] = {
                "exists": exists,
                "size_mb": round(size / (1024 * 1024), 1) if exists else 0,
                "path": str(path),
            }
            if not exists:
                result["can_skip_extract"] = False

        return result

    # ==========================================
    # CONTROL DE CONCURRENCIA
    # ==========================================

    def get_blocking_execution(self, entity: str, year: Optional[int] = None) -> Optional[Dict[str, Any]]:
        """Comprueba si hay una ejecución activa que bloquea un nuevo proceso."""
        try:
            with get_session() as session:
                query = (
                    select(EtlExecution)
                    .where(EtlExecution.entity == entity)
                    .where(EtlExecution.status.in_(["running", "interrupted"]))
                )
                if entity != "catalogos" and year is not None:
                    query = query.where(EtlExecution.year == year)
                query = query.order_by(desc(EtlExecution.started_at)).limit(1)
                execution = session.execute(query).scalar_one_or_none()
                if not execution:
                    return None
                status_label = "en ejecución" if execution.status == "running" else "interrumpido"
                return {
                    "execution_id": str(execution.id),
                    "status": execution.status,
                    "status_label": status_label,
                    "started_at": execution.started_at.isoformat() if execution.started_at else None,
                    "progress": execution.progress_percentage or 0,
                }
        except Exception:
            return None

    # ==========================================
    # LIMPIEZA DE ARCHIVOS TEMPORALES
    # ==========================================

    def _get_temp_paths(self, entity: str, year: Optional[int] = None) -> List[Path]:
        """Devuelve las rutas de archivos temporales JSONL para una entidad/año."""
        paths = []
        jsonl_dir = self.seeding_dir / "concesiones" / "data" / "jsonl"
        transformed_dir = jsonl_dir / "transformed"

        if entity in ("all_concesiones", "concesiones") and year:
            paths.extend([
                jsonl_dir / f"concesiones_ordinarias_{year}.jsonl",
                jsonl_dir / f"concesiones_minimis_{year}.jsonl",
                jsonl_dir / f"concesiones_ayudasestado_{year}.jsonl",
                jsonl_dir / f"concesiones_partidospoliticos_{year}.jsonl",
                transformed_dir / f"concesiones_{year}.jsonl",
                transformed_dir / f"beneficiarios_pendientes_{year}.jsonl",
            ])

        return paths

    def clean_temp_files(self, entity: str, year: Optional[int] = None) -> Dict[str, Any]:
        """Elimina archivos temporales JSONL de una entidad/año."""
        paths = self._get_temp_paths(entity, year)
        deleted = []
        errors = []

        for p in paths:
            if p.exists():
                try:
                    p.unlink()
                    deleted.append(str(p.name))
                except Exception as e:
                    errors.append(f"{p.name}: {e}")

        return {
            "entity": entity,
            "year": year,
            "deleted": deleted,
            "errors": errors,
        }

    # ==========================================
    # LANZAR PROCESOS
    # ==========================================

    async def start_seeding(
        self,
        year: int,
        entity: str = "all",
        batch_size: int = 5000,
        replacing_execution_id: Optional[UUID] = None
    ) -> Dict[str, Any]:
        """Inicia proceso de seeding (carga inicial) para un año."""
        # Si estamos relanzando, marcar la ejecución anterior como reemplazada
        if replacing_execution_id:
            with get_session() as session:
                old = session.get(EtlExecution, replacing_execution_id)
                if old and old.status in ("interrupted", "failed"):
                    old.status = "replaced"
                    session.commit()

        # Control de concurrencia
        blocking = self.get_blocking_execution(entity, year if entity != "catalogos" else None)
        if blocking:
            estado = blocking["status_label"]
            if entity == "catalogos":
                raise ValueError(
                    f"Ya existe un proceso de catálogos {estado}. "
                    "Debe cancelarlo o relanzarlo antes de iniciar uno nuevo."
                )
            else:
                raise ValueError(
                    f"Ya existe un proceso de {entity} {year} {estado}. "
                    "Debe cancelarlo o relanzarlo antes de iniciar uno nuevo."
                )

        # Convocatorias requieren catálogos
        if entity in ("convocatorias", "all"):
            missing = self.check_catalogos_seeded()
            if missing:
                ultima_cat = self.get_last_successful_execution("catalogos")
                if not ultima_cat:
                    tablas = ", ".join(missing.keys())
                    raise ValueError(
                        f"Debe poblar primero los catálogos ({tablas}) antes de ejecutar convocatorias"
                    )

        # Concesiones requieren convocatorias
        concesiones_entities = ["concesiones", "minimis", "ayudas_estado", "partidos_politicos", "all_concesiones"]
        if entity in concesiones_entities:
            if not self.check_convocatorias_seeded(year):
                raise ValueError(
                    f"Debe poblar antes las convocatorias del ejercicio {year}"
                )

        with get_session() as session:
            entrypoint_map = {
                "catalogos": "seeding/catalogos/load/load_all_catalogos.py",
                "convocatorias": "seeding/convocatorias/orchestrator_convocatorias.py",
                "all_concesiones": "etl_scripts/run_etl.py",
                "minimis": "seeding/minimis/extract_minimis.py",
                "ayudas_estado": "seeding/ayudas_estado/extract_ayudas_estado.py",
                "partidos_politicos": "seeding/partidos_politicos/extract_partidos_politicos.py",
            }
            entrypoint = entrypoint_map.get(entity, "etl_scripts/run_etl.py")

            execution = EtlExecution(
                execution_type="seeding",
                entity=entity,
                year=year,
                status="running",
                started_at=datetime.utcnow(),
                entrypoint=entrypoint,
                current_phase="initializing",
                progress_percentage=0
            )
            session.add(execution)
            session.commit()
            session.refresh(execution)
            execution_id = execution.id

        # Lanzar proceso en background
        script_path = self.etl_root / "run_etl.py"
        cmd = [
            sys.executable,
            str(script_path),
            "--year", str(year),
            "--execution-id", str(execution_id)
        ]

        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )

        self.active_processes[execution_id] = process
        asyncio.create_task(self._monitor_process(execution_id, process))

        return {
            "execution_id": str(execution_id),
            "status": "running",
            "year": year,
            "entity": entity,
            "started_at": execution.started_at.isoformat()
        }

    async def start_concesiones_jsonl_pipeline(
        self,
        year: int,
        skip_extract: bool = False,
        skip_transform: bool = False,
        replacing_execution_id: Optional[UUID] = None
    ) -> Dict[str, Any]:
        """Inicia pipeline completo de concesiones JSONL (extract → transform → load)."""
        # Si estamos relanzando, marcar la ejecución anterior como reemplazada
        if replacing_execution_id:
            with get_session() as session:
                old = session.get(EtlExecution, replacing_execution_id)
                if old and old.status in ("interrupted", "failed"):
                    old.status = "replaced"
                    session.commit()

        # Control de concurrencia
        blocking = self.get_blocking_execution("all_concesiones", year)
        if blocking:
            raise ValueError(
                f"Ya existe un proceso de concesiones {year} {blocking['status_label']}. "
                "Debe cancelarlo o relanzarlo antes de iniciar uno nuevo."
            )

        # Requieren convocatorias
        if not self.check_convocatorias_seeded(year):
            raise ValueError(
                f"Debe poblar antes las convocatorias del ejercicio {year}"
            )

        with get_session() as session:
            execution = EtlExecution(
                execution_type="seeding",
                entity="all_concesiones",
                year=year,
                status="running",
                started_at=datetime.utcnow(),
                entrypoint="etl_scripts/run_etl.py",
                current_phase="initializing",
                progress_percentage=0
            )
            session.add(execution)
            session.commit()
            session.refresh(execution)
            execution_id = execution.id

        script_path = self.etl_root / "run_etl.py"
        cmd = [
            sys.executable,
            str(script_path),
            "--year", str(year),
            "--execution-id", str(execution_id)
        ]

        if skip_extract:
            cmd.append("--skip-extract")
        if skip_transform:
            cmd.append("--skip-transform")

        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )

        self.active_processes[execution_id] = process
        asyncio.create_task(self._monitor_process(execution_id, process))

        return {
            "execution_id": str(execution_id),
            "status": "running",
            "year": year,
            "entity": "all_concesiones",
            "started_at": execution.started_at.isoformat()
        }

    async def start_sync(
        self,
        year: int,
        entity: str = "all",
        incremental: bool = True,
        days_back: int = 7
    ) -> Dict[str, Any]:
        """Inicia proceso de sincronización (actualización incremental)."""
        with get_session() as session:
            execution = EtlExecution(
                execution_type="sync",
                entity=entity,
                year=year,
                status="running",
                started_at=datetime.utcnow()
            )
            session.add(execution)
            session.commit()
            session.refresh(execution)
            execution_id = execution.id

        script_path = self.etl_root / "run_etl.py"
        cmd = [
            sys.executable,
            str(script_path),
            "--year", str(year),
            "--mode", "sync",
            "--entity", entity,
            "--execution-id", str(execution_id)
        ]

        if incremental:
            cmd.extend(["--incremental", "--days-back", str(days_back)])

        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )

        self.active_processes[execution_id] = process
        asyncio.create_task(self._monitor_process(execution_id, process))

        return {
            "execution_id": str(execution_id),
            "status": "running",
            "year": year,
            "entity": entity,
            "incremental": incremental,
            "started_at": execution.started_at.isoformat()
        }

    async def stop_execution(self, execution_id: UUID) -> Dict[str, Any]:
        """Detiene una ejecución en curso."""
        process = self.active_processes.get(execution_id)
        if not process:
            return {"success": False, "error": "Process not found or already finished"}

        process.terminate()
        await asyncio.sleep(1)

        if process.returncode is None:
            process.kill()

        with get_session() as session:
            execution = session.get(EtlExecution, execution_id)
            if execution:
                execution.status = "cancelled"
                execution.finished_at = datetime.utcnow()
                session.commit()

        del self.active_processes[execution_id]

        return {"success": True, "status": "cancelled"}

    def delete_execution(self, execution_id: UUID) -> Dict[str, Any]:
        """Elimina un registro de ejecución del historial."""
        with get_session() as session:
            execution = session.get(EtlExecution, execution_id)
            if not execution:
                return {"success": False, "error": "Ejecución no encontrada"}
            if execution.status == "running":
                return {"success": False, "error": "No se puede eliminar una ejecución en curso"}
            session.delete(execution)
            session.commit()
        return {"success": True}

    async def _monitor_process(self, execution_id: UUID, process: asyncio.subprocess.Process):
        """Monitorea un proceso ETL y actualiza su estado en tiempo real."""
        log_lines = []
        stderr_text = ""

        try:
            async for line in process.stdout:
                decoded_line = line.decode('utf-8').rstrip()
                log_lines.append(decoded_line)

                if len(log_lines) % 10 == 0:
                    current_log = '\n'.join(log_lines[-500:])
                    with get_session() as session:
                        execution = session.get(EtlExecution, execution_id)
                        if execution:
                            execution.log = current_log
                            session.commit()

            await process.wait()

            stderr_data = await process.stderr.read() if process.stderr else b''
            if stderr_data:
                stderr_text = stderr_data.decode('utf-8')
                log_lines.append(f"\n--- STDERR ---\n{stderr_text}")

            final_log = '\n'.join(log_lines[-1000:])
            with get_session() as session:
                execution = session.get(EtlExecution, execution_id)
                if execution:
                    execution.finished_at = datetime.utcnow()
                    execution.status = "completed" if process.returncode == 0 else "failed"
                    execution.log = final_log

                    if process.returncode != 0:
                        execution.error_message = stderr_text[:500] if stderr_text else "Process failed with no error message"

                    session.commit()
        except Exception as e:
            error_msg = f"Monitor error: {str(e)}"
            log_lines.append(error_msg)
            with get_session() as session:
                execution = session.get(EtlExecution, execution_id)
                if execution:
                    execution.finished_at = datetime.utcnow()
                    execution.status = "failed"
                    execution.log = '\n'.join(log_lines[-1000:])
                    execution.error_message = error_msg
                    session.commit()
        finally:
            if execution_id in self.active_processes:
                del self.active_processes[execution_id]

    # ==========================================
    # CONSULTAS Y ESTADÍSTICAS
    # ==========================================

    def get_execution_status(self, execution_id: UUID) -> Optional[Dict[str, Any]]:
        """Obtiene el estado de una ejecución."""
        with get_session() as session:
            execution = session.get(EtlExecution, execution_id)
            if not execution:
                return None

            jobs_stmt = select(EtlJob).where(EtlJob.execution_id == execution_id)
            jobs = session.execute(jobs_stmt).scalars().all()

            total_jobs = len(jobs)
            completed_jobs = sum(1 for j in jobs if j.status == "completed")

            progress_pct = (completed_jobs / total_jobs * 100) if total_jobs > 0 else 0

            elapsed_time = 0
            if execution.started_at:
                end_time = execution.finished_at or datetime.utcnow()
                elapsed_time = int((end_time - execution.started_at).total_seconds())

            return {
                "execution_id": str(execution.id),
                "execution_type": execution.execution_type,
                "entity": execution.entity,
                "year": execution.year,
                "status": execution.status,
                "started_at": execution.started_at.isoformat() if execution.started_at else None,
                "finished_at": execution.finished_at.isoformat() if execution.finished_at else None,
                "entrypoint": execution.entrypoint,
                "current_phase": execution.current_phase,
                "current_operation": execution.current_operation,
                "progress": execution.progress_percentage or progress_pct,
                "elapsed_time": elapsed_time,
                "records_processed": execution.records_processed or 0,
                "records_inserted": execution.records_inserted or 0,
                "records_updated": execution.records_updated or 0,
                "records_errors": execution.records_errors or 0,
                "error_message": execution.error_message,
                "log": execution.log,
                "stats": {
                    "records_processed": execution.records_processed or 0,
                    "records_inserted": execution.records_inserted or 0,
                    "records_updated": execution.records_updated or 0,
                    "records_errors": execution.records_errors or 0
                }
            }

    def list_recent_executions(self, limit: int = 20) -> List[Dict[str, Any]]:
        """Lista las ejecuciones más recientes."""
        with get_session() as session:
            stmt = (
                select(EtlExecution)
                .order_by(desc(EtlExecution.started_at))
                .limit(limit)
            )
            executions = session.execute(stmt).scalars().all()

            result = []
            for execution in executions:
                duration = None
                elapsed_time = 0
                if execution.started_at:
                    end_time = execution.finished_at or datetime.utcnow()
                    elapsed_time = int((end_time - execution.started_at).total_seconds())
                    if execution.finished_at:
                        duration = elapsed_time

                result.append({
                    "execution_id": str(execution.id),
                    "execution_type": execution.execution_type,
                    "entity": execution.entity,
                    "year": execution.year,
                    "status": execution.status,
                    "started_at": execution.started_at.isoformat() if execution.started_at else None,
                    "finished_at": execution.finished_at.isoformat() if execution.finished_at else None,
                    "duration_seconds": duration,
                    "elapsed_time": elapsed_time,
                    "entrypoint": execution.entrypoint,
                    "current_phase": execution.current_phase,
                    "progress": execution.progress_percentage or 0,
                    "records_processed": execution.records_processed or 0,
                    "records_inserted": execution.records_inserted or 0,
                    "records_updated": execution.records_updated or 0,
                    "records_errors": execution.records_errors or 0,
                    "log": execution.log,
                    "stats": {
                        "records_processed": execution.records_processed or 0,
                        "records_inserted": execution.records_inserted or 0,
                        "records_updated": execution.records_updated or 0,
                        "records_errors": execution.records_errors or 0
                    }
                })

            return result

    def get_statistics_summary(self) -> Dict[str, Any]:
        """Obtiene estadísticas generales de ETL."""
        with get_session() as session:
            seeding_count = session.execute(
                select(func.count(EtlExecution.id)).where(EtlExecution.execution_type == "seeding")
            ).scalar()

            sync_count = session.execute(
                select(func.count(EtlExecution.id)).where(EtlExecution.execution_type == "sync")
            ).scalar()

            completed_count = session.execute(
                select(func.count(EtlExecution.id)).where(EtlExecution.status == "completed")
            ).scalar()

            failed_count = session.execute(
                select(func.count(EtlExecution.id)).where(EtlExecution.status == "failed")
            ).scalar()

            last_seeding = session.execute(
                select(EtlExecution)
                .where(
                    EtlExecution.execution_type == "seeding",
                    EtlExecution.status == "completed"
                )
                .order_by(desc(EtlExecution.finished_at))
                .limit(1)
            ).scalar_one_or_none()

            last_sync = session.execute(
                select(EtlExecution)
                .where(
                    EtlExecution.execution_type == "sync",
                    EtlExecution.status == "completed"
                )
                .order_by(desc(EtlExecution.finished_at))
                .limit(1)
            ).scalar_one_or_none()

            active_count = session.execute(
                select(func.count(EtlExecution.id)).where(EtlExecution.status == "running")
            ).scalar()

            return {
                "total_executions": {
                    "seeding": seeding_count,
                    "sync": sync_count,
                    "total": seeding_count + sync_count,
                    "completed": completed_count,
                    "failed": failed_count
                },
                "last_successful": {
                    "seeding": last_seeding.finished_at.isoformat() if last_seeding and last_seeding.finished_at else None,
                    "sync": last_sync.finished_at.isoformat() if last_sync and last_sync.finished_at else None
                },
                "active_processes": active_count
            }

    def list_active_executions(self) -> List[Dict[str, Any]]:
        """Lista las ejecuciones actualmente en curso."""
        with get_session() as session:
            stmt = (
                select(EtlExecution)
                .where(EtlExecution.status == "running")
                .order_by(desc(EtlExecution.started_at))
            )
            executions = session.execute(stmt).scalars().all()

            result = []
            for execution in executions:
                elapsed_time = 0
                if execution.started_at:
                    elapsed_time = int((datetime.utcnow() - execution.started_at).total_seconds())

                result.append({
                    "execution_id": str(execution.id),
                    "execution_type": execution.execution_type,
                    "entity": execution.entity,
                    "year": execution.year,
                    "status": execution.status,
                    "started_at": execution.started_at.isoformat() if execution.started_at else None,
                    "entrypoint": execution.entrypoint,
                    "current_phase": execution.current_phase,
                    "current_operation": execution.current_operation,
                    "progress": execution.progress_percentage or 0,
                    "elapsed_time": elapsed_time,
                    "records_processed": execution.records_processed or 0,
                    "records_inserted": execution.records_inserted or 0,
                    "records_updated": execution.records_updated or 0,
                    "records_errors": execution.records_errors or 0,
                    "log": execution.log
                })

            return result

    def get_sync_control_status(self) -> List[Dict[str, Any]]:
        """Obtiene el estado de sincronización por entidad."""
        with get_session() as session:
            stmt = select(SyncControl).order_by(SyncControl.entity)
            controls = session.execute(stmt).scalars().all()

            result = []
            for control in controls:
                result.append({
                    "entity": control.entity,
                    "last_sync_at": control.last_sync_at.isoformat() if control.last_sync_at else None,
                    "last_success_at": control.last_success_at.isoformat() if control.last_success_at else None,
                    "sync_count": control.sync_count,
                    "status": control.status,
                    "last_error": control.last_error,
                    "metadata": control.metadata or {}
                })

            return result


# Singleton
etl_service = ETLService()
