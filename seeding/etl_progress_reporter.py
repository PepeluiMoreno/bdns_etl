"""
Utilidad para reportar progreso de scripts ETL a la base de datos.

Los scripts ETL pueden usar esta clase para actualizar su progreso en tiempo real,
permitiendo que el backend y frontend muestren el estado actualizado.

Ejemplo de uso:
    from etl_progress_reporter import ETLProgressReporter

    reporter = ETLProgressReporter(execution_id)

    reporter.set_phase("extracting", "Descargando datos de API BDNS")
    reporter.update_progress(25, records_processed=1000)

    reporter.set_phase("transforming", "Normalizando datos")
    reporter.update_progress(50, records_processed=2000)

    reporter.set_phase("loading", "Insertando en base de datos")
    reporter.update_progress(75, records_inserted=1500)

    reporter.complete(records_processed=4000, records_inserted=3800)
"""

import sys
from datetime import datetime
from pathlib import Path
from typing import Optional
from uuid import UUID

# Añadir path a bdns_core
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root / "bdns_core" / "bdns_core" / "src"))

from bdns_core.db.session import get_session
from bdns_core.db.etl_models import EtlExecution


class ETLProgressReporter:
    """Reporter de progreso para scripts ETL."""

    def __init__(self, execution_id: UUID):
        """
        Inicializa el reporter.

        Args:
            execution_id: UUID de la ejecución ETL
        """
        self.execution_id = execution_id
        self._last_update = datetime.utcnow()
        self._update_interval = 2  # Segundos entre actualizaciones

    def _should_update(self) -> bool:
        """Verifica si debe actualizar (para evitar sobrecarga de BD)."""
        now = datetime.utcnow()
        elapsed = (now - self._last_update).total_seconds()
        return elapsed >= self._update_interval

    def update_progress(
        self,
        progress_percentage: Optional[int] = None,
        records_processed: Optional[int] = None,
        records_inserted: Optional[int] = None,
        records_updated: Optional[int] = None,
        records_errors: Optional[int] = None,
        force: bool = False
    ):
        """
        Actualiza el progreso de la ejecución.

        Args:
            progress_percentage: Porcentaje de progreso (0-100)
            records_processed: Total de registros procesados
            records_inserted: Registros insertados
            records_updated: Registros actualizados
            records_errors: Registros con errores
            force: Forzar actualización sin importar intervalo
        """
        if not force and not self._should_update():
            return

        try:
            with get_session() as session:
                execution = session.get(EtlExecution, self.execution_id)
                if not execution:
                    return

                if progress_percentage is not None:
                    execution.progress_percentage = min(100, max(0, progress_percentage))

                if records_processed is not None:
                    execution.records_processed = records_processed

                if records_inserted is not None:
                    execution.records_inserted = records_inserted

                if records_updated is not None:
                    execution.records_updated = records_updated

                if records_errors is not None:
                    execution.records_errors = records_errors

                session.commit()
                self._last_update = datetime.utcnow()

        except Exception as e:
            # No fallar el ETL por errores de reporting
            print(f"Warning: Error updating progress: {e}", file=sys.stderr)

    def set_phase(self, phase: str, operation: Optional[str] = None):
        """
        Establece la fase actual del ETL.

        Args:
            phase: Fase actual (extracting, transforming, loading, validating)
            operation: Descripción de la operación específica
        """
        try:
            with get_session() as session:
                execution = session.get(EtlExecution, self.execution_id)
                if not execution:
                    return

                execution.current_phase = phase
                if operation:
                    execution.current_operation = operation

                session.commit()

        except Exception as e:
            print(f"Warning: Error setting phase: {e}", file=sys.stderr)

    def set_operation(self, operation: str):
        """
        Actualiza la operación actual sin cambiar la fase.

        Args:
            operation: Descripción de la operación
        """
        try:
            with get_session() as session:
                execution = session.get(EtlExecution, self.execution_id)
                if not execution:
                    return

                execution.current_operation = operation
                session.commit()

        except Exception as e:
            print(f"Warning: Error setting operation: {e}", file=sys.stderr)

    def complete(
        self,
        records_processed: int = 0,
        records_inserted: int = 0,
        records_updated: int = 0,
        records_errors: int = 0
    ):
        """
        Marca la ejecución como completada.

        Args:
            records_processed: Total de registros procesados
            records_inserted: Registros insertados
            records_updated: Registros actualizados
            records_errors: Registros con errores
        """
        try:
            with get_session() as session:
                execution = session.get(EtlExecution, self.execution_id)
                if not execution:
                    return

                execution.status = "completed"
                execution.finished_at = datetime.utcnow()
                execution.progress_percentage = 100
                execution.current_phase = "completed"
                execution.current_operation = "Proceso completado"

                execution.records_processed = records_processed
                execution.records_inserted = records_inserted
                execution.records_updated = records_updated
                execution.records_errors = records_errors

                # Calcular tiempo de ejecución
                if execution.started_at:
                    delta = execution.finished_at - execution.started_at
                    execution.execution_time_seconds = int(delta.total_seconds())

                session.commit()

        except Exception as e:
            print(f"Warning: Error completing execution: {e}", file=sys.stderr)

    def fail(self, error_message: str):
        """
        Marca la ejecución como fallida.

        Args:
            error_message: Mensaje de error
        """
        try:
            with get_session() as session:
                execution = session.get(EtlExecution, self.execution_id)
                if not execution:
                    return

                execution.status = "failed"
                execution.finished_at = datetime.utcnow()
                execution.error_message = error_message[-500:]  # Limitar a 500 chars
                execution.current_phase = "failed"

                # Calcular tiempo de ejecución
                if execution.started_at:
                    delta = execution.finished_at - execution.started_at
                    execution.execution_time_seconds = int(delta.total_seconds())

                session.commit()

        except Exception as e:
            print(f"Warning: Error marking as failed: {e}", file=sys.stderr)


# Ejemplo de uso
if __name__ == "__main__":
    import time
    from uuid import uuid4

    # Crear una ejecución de prueba
    test_id = uuid4()

    print(f"Testing progress reporter with ID: {test_id}")

    reporter = ETLProgressReporter(test_id)

    # Simular fases del ETL
    reporter.set_phase("extracting", "Descargando datos de API")
    for i in range(0, 26, 5):
        reporter.update_progress(i, records_processed=i * 100)
        time.sleep(1)

    reporter.set_phase("transforming", "Normalizando datos")
    for i in range(25, 51, 5):
        reporter.update_progress(i, records_processed=i * 100)
        time.sleep(1)

    reporter.set_phase("loading", "Insertando en BD")
    for i in range(50, 101, 10):
        reporter.update_progress(
            i,
            records_processed=i * 100,
            records_inserted=i * 95
        )
        time.sleep(1)

    reporter.complete(
        records_processed=10000,
        records_inserted=9500,
        records_errors=5
    )

    print("Test completed!")
