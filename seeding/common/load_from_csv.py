#!/usr/bin/env python3
"""
Carga CSVs a PostgreSQL usando COPY para máximo rendimiento.

Usa COPY FROM para inserción masiva ~100x más rápida que INSERT.

Maneja:
- Deduplicación automática con ON CONFLICT DO NOTHING
- Creación de tabla temporal para staging
- Bulk upsert desde staging a tabla final

Uso:
    python load_from_csv.py /path/to/csv_dir/
"""
import sys
import time
from pathlib import Path
from typing import Tuple

PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from bdns_core.db.session import get_session
from sqlalchemy import text


def log(msg: str):
    """Log con timestamp."""
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def copy_beneficiarios(session, csv_path: Path) -> Tuple[int, int]:
    """
    Carga beneficiarios desde CSV usando COPY.

    Returns:
        (insertados, duplicados)
    """
    log(f"Cargando beneficiarios desde {csv_path.name}...")

    # 1. Crear tabla temporal
    session.execute(text("""
        CREATE TEMP TABLE IF NOT EXISTS temp_beneficiarios (
            id UUID,
            nif VARCHAR,
            nombre VARCHAR,
            nombre_norm VARCHAR,
            created_by VARCHAR,
            created_at TIMESTAMP
        ) ON COMMIT DROP;
    """))

    # 2. COPY a tabla temporal
    # Uso raw connection para COPY
    connection = session.connection()
    raw_conn = connection.connection  # psycopg2 connection
    cursor = raw_conn.cursor()

    with open(csv_path, 'r', encoding='utf-8') as f:
        # Saltar header
        next(f)
        # COPY desde archivo
        cursor.copy_expert(
            """
            COPY temp_beneficiarios (id, nif, nombre, nombre_norm, created_by, created_at)
            FROM STDIN WITH (
                FORMAT CSV,
                DELIMITER '|',
                NULL '\\N',
                QUOTE '"',
                ESCAPE '\\'
            )
            """,
            f
        )

    rows_copied = cursor.rowcount
    log(f"  ✓ COPY: {rows_copied} filas copiadas a temp")

    # 3. Upsert a tabla final con ON CONFLICT
    result = session.execute(text("""
        INSERT INTO bdns.beneficiario (id, nif, nombre, nombre_norm, created_by, created_at)
        SELECT id, nif, nombre, nombre_norm, created_by, created_at::timestamp
        FROM temp_beneficiarios
        ON CONFLICT (nif) DO NOTHING;
    """))

    insertados = result.rowcount
    duplicados = rows_copied - insertados

    session.commit()
    log(f"  ✓ Insertados: {insertados}, Duplicados: {duplicados}")

    return insertados, duplicados


def copy_convocatorias(session, csv_path: Path) -> Tuple[int, int]:
    """
    Carga convocatorias desde CSV usando COPY.

    Returns:
        (insertados, duplicados)
    """
    log(f"Cargando convocatorias desde {csv_path.name}...")

    # 1. Crear tabla temporal
    session.execute(text("""
        CREATE TEMP TABLE IF NOT EXISTS temp_convocatorias (
            id UUID,
            id_bdns VARCHAR,
            codigo_bdns VARCHAR,
            titulo VARCHAR,
            created_by VARCHAR,
            created_at TIMESTAMP
        ) ON COMMIT DROP;
    """))

    # 2. COPY a tabla temporal
    connection = session.connection()
    raw_conn = connection.connection
    cursor = raw_conn.cursor()

    with open(csv_path, 'r', encoding='utf-8') as f:
        next(f)  # Saltar header
        cursor.copy_expert(
            """
            COPY temp_convocatorias (id, id_bdns, codigo_bdns, titulo, created_by, created_at)
            FROM STDIN WITH (
                FORMAT CSV,
                DELIMITER '|',
                NULL '\\N',
                QUOTE '"',
                ESCAPE '\\'
            )
            """,
            f
        )

    rows_copied = cursor.rowcount
    log(f"  ✓ COPY: {rows_copied} filas copiadas a temp")

    # 3. Upsert a tabla final
    # codigoBDNS es el identificador natural único
    result = session.execute(text("""
        INSERT INTO bdns.convocatoria (id, id_bdns, codigo_bdns, titulo, created_by, created_at)
        SELECT id, id_bdns, codigo_bdns, titulo, created_by, created_at::timestamp
        FROM temp_convocatorias
        ON CONFLICT (codigo_bdns) DO NOTHING;
    """))

    insertados = result.rowcount
    duplicados = rows_copied - insertados

    session.commit()
    log(f"  ✓ Insertados: {insertados}, Duplicados: {duplicados}")

    return insertados, duplicados


def copy_concesiones(session, csv_path: Path) -> Tuple[int, int]:
    """
    Carga concesiones desde CSV usando COPY.

    Returns:
        (insertados, duplicados)
    """
    log(f"Cargando concesiones desde {csv_path.name}...")

    # 1. Crear tabla temporal
    session.execute(text("""
        CREATE TEMP TABLE IF NOT EXISTS temp_concesiones (
            id UUID,
            id_concesion VARCHAR,
            beneficiario_id UUID,
            convocatoria_id UUID,
            fecha_concesion DATE,
            regimen_tipo VARCHAR,
            importe_nominal FLOAT,
            importe_equivalente FLOAT,
            created_by VARCHAR,
            created_at TIMESTAMP
        ) ON COMMIT DROP;
    """))

    # 2. COPY a tabla temporal
    connection = session.connection()
    raw_conn = connection.connection
    cursor = raw_conn.cursor()

    with open(csv_path, 'r', encoding='utf-8') as f:
        next(f)  # Saltar header
        cursor.copy_expert(
            """
            COPY temp_concesiones (
                id, id_concesion, beneficiario_id, convocatoria_id,
                fecha_concesion, regimen_tipo, importe_nominal, importe_equivalente,
                created_by, created_at
            )
            FROM STDIN WITH (
                FORMAT CSV,
                DELIMITER '|',
                NULL '\\N',
                QUOTE '"',
                ESCAPE '\\'
            )
            """,
            f
        )

    rows_copied = cursor.rowcount
    log(f"  ✓ COPY: {rows_copied} filas copiadas a temp")

    # 3. Upsert a tabla final particionada
    # UNIQUE constraint: (id_concesion, fecha_concesion, regimen_tipo)
    result = session.execute(text("""
        INSERT INTO bdns.concesion (
            id, id_concesion, beneficiario_id, convocatoria_id,
            fecha_concesion, regimen_tipo, importe_nominal, importe_equivalente,
            created_by, created_at
        )
        SELECT
            id, id_concesion, beneficiario_id, convocatoria_id,
            fecha_concesion, regimen_tipo, importe_nominal, importe_equivalente,
            created_by, created_at::timestamp
        FROM temp_concesiones
        ON CONFLICT (id_concesion, fecha_concesion, regimen_tipo) DO NOTHING;
    """))

    insertados = result.rowcount
    duplicados = rows_copied - insertados

    session.commit()
    log(f"  ✓ Insertados: {insertados}, Duplicados: {duplicados}")

    return insertados, duplicados


def load_csvs(csv_dir: Path):
    """Carga todos los CSVs de un directorio."""
    benef_csv = csv_dir / 'beneficiarios.csv'
    conv_csv = csv_dir / 'convocatorias.csv'
    conc_csv = csv_dir / 'concesiones.csv'

    # Validar que existen
    missing = []
    if not benef_csv.exists():
        missing.append(str(benef_csv))
    if not conv_csv.exists():
        missing.append(str(conv_csv))
    if not conc_csv.exists():
        missing.append(str(conc_csv))

    if missing:
        log(f"ERROR: CSVs faltantes:")
        for m in missing:
            log(f"  - {m}")
        sys.exit(1)

    log(f"{'='*60}")
    log(f"LOAD CSV → PostgreSQL (COPY)")
    log(f"{'='*60}")
    log(f"Directorio: {csv_dir}")
    log("")

    start_time = time.time()

    with get_session() as session:
        # 1. Beneficiarios (primero, FK dependency)
        b_ins, b_dup = copy_beneficiarios(session, benef_csv)

        # 2. Convocatorias
        c_ins, c_dup = copy_convocatorias(session, conv_csv)

        # 3. Concesiones
        conc_ins, conc_dup = copy_concesiones(session, conc_csv)

    elapsed = time.time() - start_time

    log(f"\n{'='*60}")
    log("✓ CARGA COMPLETADA")
    log(f"{'='*60}")
    log(f"Beneficiarios:  {b_ins:>8} insertados, {b_dup:>6} duplicados")
    log(f"Convocatorias:  {c_ins:>8} insertados, {c_dup:>6} duplicados")
    log(f"Concesiones:    {conc_ins:>8} insertados, {conc_dup:>6} duplicados")
    log(f"")
    log(f"Tiempo total:   {elapsed:.1f}s")
    log(f"Velocidad:      {conc_ins / elapsed:.0f} concesiones/s")
    log(f"{'='*60}\n")


def main():
    if len(sys.argv) < 2:
        print("Uso: python load_from_csv.py <csv_directory>")
        print()
        print("El directorio debe contener:")
        print("  - beneficiarios.csv")
        print("  - convocatorias.csv")
        print("  - concesiones.csv")
        print()
        print("Ejemplo:")
        print("  python load_from_csv.py ./output/")
        sys.exit(1)

    csv_dir = Path(sys.argv[1])

    if not csv_dir.is_dir():
        print(f"Error: No es un directorio: {csv_dir}")
        sys.exit(1)

    load_csvs(csv_dir)


if __name__ == "__main__":
    main()
