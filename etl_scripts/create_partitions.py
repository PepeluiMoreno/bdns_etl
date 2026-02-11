#!/usr/bin/env python3
"""
Crea particiones para la tabla concesion.

Estructura jerárquica:
- Nivel 1: RANGE por fecha_concesion (anual)
- Nivel 2: LIST por regimen_tipo

Ejemplo:
    python create_partitions.py --year 2024
    python create_partitions.py --year 2015 --year 2024  # Rango
"""
import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from bdns_core.db.session import get_session
from sqlalchemy import text


REGIMEN_TIPOS = [
    'minimis',
    'ayuda_estado',
    'ordinaria',
    'partidos_politicos',
    'grandes_benef',  # Acortado para caber en VARCHAR(20)
    'desconocido'
]


def create_year_partition(session, year: int):
    """
    Crea partición para un año específico.

    Estructura:
    1. Partición padre para el año (RANGE)
    2. Sub-particiones para cada regimen_tipo (LIST)
    """
    # Nombre de la partición padre
    parent_partition = f"concesion_{year}"

    # Fechas de rango
    start_date = f"{year}-01-01"
    end_date = f"{year + 1}-01-01"

    print(f"\n{'='*60}")
    print(f"Creando particiones para año {year}")
    print(f"{'='*60}")

    # 1. Crear partición padre RANGE por fecha_concesion
    try:
        sql_parent = f"""
        CREATE TABLE IF NOT EXISTS bdns.{parent_partition}
        PARTITION OF bdns.concesion
        FOR VALUES FROM ('{start_date}') TO ('{end_date}')
        PARTITION BY LIST (regimen_tipo);
        """

        session.execute(text(sql_parent))
        session.commit()
        print(f"✓ Partición padre creada: {parent_partition}")
    except Exception as e:
        if "already exists" in str(e):
            print(f"⚠ Partición {parent_partition} ya existe")
            session.rollback()
        else:
            print(f"✗ Error creando partición padre: {e}")
            session.rollback()
            return False

    # 2. Crear sub-particiones LIST por regimen_tipo
    for regimen in REGIMEN_TIPOS:
        child_partition = f"{parent_partition}_{regimen}"

        try:
            sql_child = f"""
            CREATE TABLE IF NOT EXISTS bdns.{child_partition}
            PARTITION OF bdns.{parent_partition}
            FOR VALUES IN ('{regimen}');
            """

            session.execute(text(sql_child))
            session.commit()
            print(f"  ✓ Sub-partición creada: {child_partition} (regimen_tipo='{regimen}')")
        except Exception as e:
            if "already exists" in str(e):
                print(f"  ⚠ Sub-partición {child_partition} ya existe")
                session.rollback()
            else:
                print(f"  ✗ Error creando sub-partición {child_partition}: {e}")
                session.rollback()

    return True


def list_partitions(session):
    """Lista todas las particiones existentes."""
    sql = """
    SELECT
        schemaname,
        tablename,
        pg_get_expr(c.relpartbound, c.oid) as partition_expression
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    JOIN pg_tables t ON c.relname = t.tablename AND n.nspname = t.schemaname
    WHERE c.relispartition = true
      AND n.nspname = 'bdns'
      AND c.relname LIKE 'concesion%'
    ORDER BY tablename;
    """

    result = session.execute(text(sql))
    rows = result.fetchall()

    print(f"\n{'='*60}")
    print(f"Particiones existentes ({len(rows)})")
    print(f"{'='*60}")

    for row in rows:
        print(f"  {row[1]:40} → {row[2]}")


def main():
    parser = argparse.ArgumentParser(description='Crear particiones para tabla concesion')
    parser.add_argument('--year', type=int, action='append', help='Año(s) para crear particiones')
    parser.add_argument('--year-range', type=int, nargs=2, metavar=('START', 'END'),
                       help='Rango de años (inclusivo)')
    parser.add_argument('--list', action='store_true', help='Listar particiones existentes')
    args = parser.parse_args()

    with get_session() as session:
        # Listar particiones existentes
        if args.list:
            list_partitions(session)
            return 0

        # Determinar años a procesar
        years = []
        if args.year:
            years.extend(args.year)
        if args.year_range:
            start, end = args.year_range
            years.extend(range(start, end + 1))

        if not years:
            print("Error: Especifica --year o --year-range")
            parser.print_help()
            return 1

        # Crear particiones para cada año
        for year in sorted(set(years)):
            if year < 2000 or year > 2030:
                print(f"⚠ Año fuera de rango válido: {year} (saltado)")
                continue

            create_year_partition(session, year)

        # Listar particiones al final
        list_partitions(session)

        print(f"\n{'='*60}")
        print("✓ Proceso completado")
        print(f"{'='*60}\n")

    return 0


if __name__ == "__main__":
    sys.exit(main())
