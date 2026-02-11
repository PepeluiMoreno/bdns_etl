"""
Load: carga CSV de convocatorias a PostgreSQL usando COPY.

Usa tabla temporal + INSERT ON CONFLICT (id_bdns) DO NOTHING
para deduplicación automática.

Requiere CSV pipe-separated generado por transform_convocatorias_to_csv.py
"""

import sys
import time
from pathlib import Path
from typing import Tuple

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from sqlalchemy import text
from bdns_core.db.session import get_session


TEMP_TABLE_DDL = """
    CREATE TEMP TABLE IF NOT EXISTS temp_convocatorias (
        id UUID,
        id_bdns VARCHAR,
        codigo_bdns VARCHAR,
        titulo VARCHAR,
        descripcion TEXT,
        fecha_recepcion DATE,
        fecha_publicacion DATE,
        organo_id UUID,
        presupuesto_total FLOAT,
        reglamento_id UUID,
        created_by VARCHAR,
        created_at TIMESTAMP
    ) ON COMMIT DROP;
"""

COPY_COLUMNS = (
    "id, id_bdns, codigo_bdns, titulo, descripcion, "
    "fecha_recepcion, fecha_publicacion, organo_id, "
    "presupuesto_total, reglamento_id, created_by, created_at"
)

UPSERT_SQL = f"""
    INSERT INTO bdns.convocatoria ({COPY_COLUMNS})
    SELECT {COPY_COLUMNS}
    FROM temp_convocatorias
    ON CONFLICT (id_bdns) DO NOTHING;
"""


def log(msg: str):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def copy_convocatorias(csv_path: Path) -> Tuple[int, int]:
    """
    Carga convocatorias desde CSV pipe-separated usando COPY.

    Args:
        csv_path: Ruta al CSV generado por transform_convocatorias_to_csv.py

    Returns:
        (insertados, duplicados)
    """
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV no encontrado: {csv_path}")

    log(f"Cargando convocatorias desde {csv_path.name}...")

    with get_session() as session:
        # 1. Crear tabla temporal
        session.execute(text(TEMP_TABLE_DDL))

        # 2. COPY a tabla temporal via raw psycopg2
        connection = session.connection()
        raw_conn = connection.connection
        cursor = raw_conn.cursor()

        with open(csv_path, "r", encoding="utf-8") as f:
            next(f)  # Saltar header
            cursor.copy_expert(
                f"""
                COPY temp_convocatorias ({COPY_COLUMNS})
                FROM STDIN WITH (
                    FORMAT CSV,
                    DELIMITER '|',
                    NULL '\\N',
                    QUOTE '"',
                    ESCAPE '\\'
                )
                """,
                f,
            )

        rows_copied = cursor.rowcount
        log(f"  COPY: {rows_copied} filas copiadas a temp")

        # 3. Upsert a tabla final con ON CONFLICT (id_bdns)
        result = session.execute(text(UPSERT_SQL))

        insertados = result.rowcount
        duplicados = rows_copied - insertados

        session.commit()
        log(f"  Insertados: {insertados}, Duplicados: {duplicados}")

    return insertados, duplicados


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Carga CSV de convocatorias a BD usando COPY"
    )
    parser.add_argument("--csv", required=True, help="CSV pipe-separated de entrada")
    args = parser.parse_args()
    ins, dup = copy_convocatorias(Path(args.csv))
    print(f"Resultado: {ins} insertados, {dup} duplicados")
