"""
Servicio para gestión y monitoreo de procesos ETL.

Proporciona funcionalidades para:
- Lanzar procesos ETL (seeding y sync)
- Monitorear progreso en tiempo real
- Consultar estadísticas de ejecuciones
- Gestionar jobs y executions
"""
import asyncio
import subprocess
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional
from uuid import UUID

from sqlalchemy import select, func, desc
from sqlalchemy.orm import Session

from bdns_core.db.session import get_session
from bdns_core.db.etl_models import EtlJob, EtlExecution, SyncControl


class ETLService:
    """Servicio para gestión de procesos ETL."""

    def __init__(self):
        self.etl_root = Path(__file__).parent.parent.parent.parent.parent / "ETL"
        self.active_processes: Dict[UUID, subprocess.Popen] = {}

    # ==========================================
    # LANZAR PROCESOS
    # ==========================================

    async def start_seeding(
        self,
        year: int,
        entity: str = "all",
        batch_size: int = 5000
    ) -> Dict[str, Any]:
        """
        Inicia proceso de seeding (carga inicial) para un año.

        Args:
            year: Año a procesar
            entity: Entidad específica ('convocatorias', 'concesiones', 'all')
            batch_size: Tamaño de batch para carga

        Returns:
            Dict con execution_id y metadata del proceso
        """
        with get_session() as session:
            # Crear registro de ejecución
            execution = EtlExecution(
                process_type="seeding",
                entity=entity,
                year=year,
                status="running",
                started_at=datetime.utcnow(),
                config={
                    "batch_size": batch_size,
                    "mode": "full_load"
                }
            )
            session.add(execution)
            session.commit()
            session.refresh(execution)
            execution_id = execution.id

        # Lanzar proceso en background
        script_path = self.etl_root / "run_etl.py"
        cmd = [
            "python",
            str(script_path),
            "--year", str(year),
            "--mode", "seeding",
            "--entity", entity,
            "--batch-size", str(batch_size),
            "--execution-id", str(execution_id)
        ]

        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )

        self.active_processes[execution_id] = process

        # Monitorear proceso en background
        asyncio.create_task(self._monitor_process(execution_id, process))

        return {
            "execution_id": str(execution_id),
            "status": "running",
            "year": year,
            "entity": entity,
            "started_at": execution.started_at.isoformat()
        }

    async def start_sync(
        self,
        year: int,
        entity: str = "all",
        incremental: bool = True,
        days_back: int = 7
    ) -> Dict[str, Any]:
        """
        Inicia proceso de sincronización (actualización incremental).

        Args:
            year: Año a sincronizar
            entity: Entidad específica ('convocatorias', 'concesiones', 'all')
            incremental: Si True, solo sincroniza cambios recientes
            days_back: Días hacia atrás para sync incremental

        Returns:
            Dict con execution_id y metadata del proceso
        """
        with get_session() as session:
            execution = EtlExecution(
                process_type="sync",
                entity=entity,
                year=year,
                status="running",
                started_at=datetime.utcnow(),
                config={
                    "incremental": incremental,
                    "days_back": days_back
                }
            )
            session.add(execution)
            session.commit()
            session.refresh(execution)
            execution_id = execution.id

        script_path = self.etl_root / "run_etl.py"
        cmd = [
            "python",
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

    async def _monitor_process(self, execution_id: UUID, process: asyncio.subprocess.Process):
        """Monitorea un proceso ETL y actualiza su estado."""
        try:
            stdout, stderr = await process.communicate()

            with get_session() as session:
                execution = session.get(EtlExecution, execution_id)
                if execution:
                    execution.finished_at = datetime.utcnow()
                    execution.status = "completed" if process.returncode == 0 else "failed"

                    if process.returncode != 0:
                        execution.error = stderr.decode('utf-8')[-500:]  # Últimos 500 chars

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

            # Obtener jobs asociados
            jobs_stmt = select(EtlJob).where(EtlJob.execution_id == execution_id)
            jobs = session.execute(jobs_stmt).scalars().all()

            total_jobs = len(jobs)
            completed_jobs = sum(1 for j in jobs if j.status == "completed")
            failed_jobs = sum(1 for j in jobs if j.status == "failed")

            progress_pct = (completed_jobs / total_jobs * 100) if total_jobs > 0 else 0

            return {
                "execution_id": str(execution.id),
                "process_type": execution.process_type,
                "entity": execution.entity,
                "year": execution.year,
                "status": execution.status,
                "started_at": execution.started_at.isoformat() if execution.started_at else None,
                "finished_at": execution.finished_at.isoformat() if execution.finished_at else None,
                "progress": {
                    "total_jobs": total_jobs,
                    "completed": completed_jobs,
                    "failed": failed_jobs,
                    "running": total_jobs - completed_jobs - failed_jobs,
                    "percentage": round(progress_pct, 2)
                },
                "stats": execution.stats or {},
                "config": execution.config or {},
                "error": execution.error
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
                # Calcular duración
                duration = None
                if execution.started_at and execution.finished_at:
                    delta = execution.finished_at - execution.started_at
                    duration = delta.total_seconds()

                result.append({
                    "execution_id": str(execution.id),
                    "process_type": execution.process_type,
                    "entity": execution.entity,
                    "year": execution.year,
                    "status": execution.status,
                    "started_at": execution.started_at.isoformat() if execution.started_at else None,
                    "finished_at": execution.finished_at.isoformat() if execution.finished_at else None,
                    "duration_seconds": duration,
                    "stats": execution.stats or {}
                })

            return result

    def get_statistics_summary(self) -> Dict[str, Any]:
        """Obtiene estadísticas generales de ETL."""
        with get_session() as session:
            # Total executions por tipo
            seeding_count = session.execute(
                select(func.count(EtlExecution.id)).where(EtlExecution.process_type == "seeding")
            ).scalar()

            sync_count = session.execute(
                select(func.count(EtlExecution.id)).where(EtlExecution.process_type == "sync")
            ).scalar()

            # Últimas ejecuciones exitosas
            last_seeding = session.execute(
                select(EtlExecution)
                .where(
                    EtlExecution.process_type == "seeding",
                    EtlExecution.status == "completed"
                )
                .order_by(desc(EtlExecution.finished_at))
                .limit(1)
            ).scalar_one_or_none()

            last_sync = session.execute(
                select(EtlExecution)
                .where(
                    EtlExecution.process_type == "sync",
                    EtlExecution.status == "completed"
                )
                .order_by(desc(EtlExecution.finished_at))
                .limit(1)
            ).scalar_one_or_none()

            # Procesos activos
            active_count = session.execute(
                select(func.count(EtlExecution.id)).where(EtlExecution.status == "running")
            ).scalar()

            return {
                "total_executions": {
                    "seeding": seeding_count,
                    "sync": sync_count,
                    "total": seeding_count + sync_count
                },
                "last_successful": {
                    "seeding": last_seeding.finished_at.isoformat() if last_seeding and last_seeding.finished_at else None,
                    "sync": last_sync.finished_at.isoformat() if last_sync and last_sync.finished_at else None
                },
                "active_processes": active_count
            }

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


# Singleton instance
etl_service = ETLService()
