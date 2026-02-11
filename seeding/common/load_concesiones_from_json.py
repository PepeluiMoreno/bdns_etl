#!/usr/bin/env python3
"""
Loader genérico de concesiones desde JSONs.

Carga concesiones de diferentes fuentes:
- Minimis
- Ayudas de Estado
- Partidos Políticos
- Grandes Beneficiarios

Usa deduplicación automática con ON CONFLICT DO NOTHING.
"""

import json
import sys
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime
from uuid import uuid4

# Añadir rutas al path
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from sqlalchemy.dialects.postgresql import insert as pg_insert
from bdns_core.db.session import get_session
from bdns_core.db.models import Concesion, Beneficiario, Convocatoria


def extract_nif(beneficiario_str: str) -> Optional[str]:
    """Extrae NIF del campo beneficiario (formato: 'NIF NOMBRE')."""
    if not beneficiario_str:
        return None
    parts = beneficiario_str.strip().split(maxsplit=1)
    return parts[0].strip() if parts else None


def get_or_create_beneficiario(session, beneficiario_str: str) -> Optional[str]:
    """Obtiene o crea un beneficiario. Retorna su UUID."""
    try:
        nif = extract_nif(beneficiario_str)
        if not nif or len(nif) < 5:
            return None

        # Buscar existente con SQL directo para evitar problemas de modelo
        from sqlalchemy import text
        result = session.execute(
            text("SELECT id FROM bdns.beneficiario WHERE nif = :nif"),
            {"nif": nif}
        ).first()

        if result:
            return str(result[0])

        # Crear nuevo con SQL directo
        nombre = beneficiario_str.split(maxsplit=1)[1] if ' ' in beneficiario_str else beneficiario_str
        new_id = uuid4()
        session.execute(
            text("INSERT INTO bdns.beneficiario (id, nif, nombre, created_by, created_at) VALUES (:id, :nif, :nombre, :created_by, NOW())"),
            {"id": new_id, "nif": nif, "nombre": nombre[:500] if nombre else None, "created_by": "etl_load"}
        )
        session.flush()
        return str(new_id)

    except Exception as e:
        session.rollback()
        print(f"  ⚠️  Error con beneficiario {beneficiario_str[:30]}: {e}")
        return None


def get_or_create_convocatoria(session, id_convocatoria: Any, titulo: str = None) -> Optional[str]:
    """Obtiene o crea una convocatoria. Retorna su UUID."""
    try:
        if not id_convocatoria:
            return None

        id_bdns = str(id_convocatoria)

        # Buscar existente con SQL directo
        from sqlalchemy import text
        result = session.execute(
            text("SELECT id FROM bdns.convocatoria WHERE id_bdns = :id_bdns"),
            {"id_bdns": id_bdns}
        ).first()

        if result:
            return str(result[0])

        # Crear nueva con SQL directo
        new_id = uuid4()
        session.execute(
            text("INSERT INTO bdns.convocatoria (id, id_bdns, titulo, created_by, created_at) VALUES (:id, :id_bdns, :titulo, :created_by, NOW())"),
            {"id": new_id, "id_bdns": id_bdns, "titulo": (titulo or f"Conv {id_bdns}")[:500], "created_by": "etl_load"}
        )
        session.flush()
        return str(new_id)

    except Exception as e:
        session.rollback()
        print(f"  ⚠️  Error con convocatoria {id_convocatoria}: {e}")
        return None


def load_json_to_concesiones(
    json_path: Path,
    regimen_tipo: str,
    batch_size: int = 1000
) -> Dict[str, int]:
    """
    Carga concesiones desde JSON a base de datos.

    Args:
        json_path: Ruta al JSON
        regimen_tipo: Tipo de régimen (minimis, ayuda_estado, etc.)
        batch_size: Registros por batch

    Returns:
        Estadísticas de la carga
    """
    print(f"\n📥 Cargando {json_path.name}")
    print(f"   Régimen: {regimen_tipo}")

    # Leer JSON
    with open(json_path, 'r', encoding='utf-8') as f:
        records = json.load(f)

    total = len(records)
    print(f"   Registros en JSON: {total:,}")

    stats = {
        'procesados': 0,
        'beneficiarios_nuevos': 0,
        'convocatorias_nuevas': 0,
        'concesiones_insertadas': 0,
        'duplicados': 0,
        'errores': 0
    }

    with get_session() as session:
        batch = []

        for idx, rec in enumerate(records, 1):
            try:
                # Progreso
                if idx % 100 == 0:
                    print(f"   {idx:,} / {total:,} ...")

                # Beneficiario
                beneficiario_str = rec.get('beneficiario', '')
                beneficiario_id = get_or_create_beneficiario(session, beneficiario_str)
                if not beneficiario_id:
                    stats['errores'] += 1
                    continue

                # Convocatoria
                id_conv = rec.get('idConvocatoria') or rec.get('numeroConvocatoria')
                titulo_conv = rec.get('convocatoria')
                convocatoria_id = get_or_create_convocatoria(session, id_conv, titulo_conv)
                if not convocatoria_id:
                    stats['errores'] += 1
                    continue

                # Preparar concesión
                id_concesion = (rec.get('codConcesion') or
                               rec.get('codigoConcesion') or
                               str(rec.get('id', '')))

                fecha_str = rec.get('fechaConcesion')
                if not fecha_str:
                    stats['errores'] += 1
                    continue

                fecha = datetime.strptime(fecha_str, '%Y-%m-%d').date()

                importe_eq = rec.get('ayudaEquivalente') or rec.get('ayudaETotal')
                importe_nom = rec.get('importe')

                concesion = {
                    'id': uuid4(),
                    'id_concesion': id_concesion,
                    'fecha_concesion': fecha,
                    'regimen_tipo': regimen_tipo,
                    'beneficiario_id': beneficiario_id,
                    'convocatoria_id': convocatoria_id,
                    'importe_equivalente': float(importe_eq) if importe_eq else None,
                    'importe_nominal': float(importe_nom) if importe_nom else None,
                    'created_by': 'etl_load'
                }

                batch.append(concesion)
                stats['procesados'] += 1

                # Insertar batch
                if len(batch) >= batch_size:
                    inserted, duplicated = _insert_batch(session, batch)
                    stats['concesiones_insertadas'] += inserted
                    stats['duplicados'] += duplicated
                    batch = []

            except Exception as e:
                print(f"   ⚠️  Error en registro {idx}: {e}")
                stats['errores'] += 1
                continue

        # Batch final
        if batch:
            inserted, duplicated = _insert_batch(session, batch)
            stats['concesiones_insertadas'] += inserted
            stats['duplicados'] += duplicated

        session.commit()

    # Resumen
    print(f"\n✅ Carga completada:")
    print(f"   Procesados: {stats['procesados']:,}")
    print(f"   Insertadas: {stats['concesiones_insertadas']:,}")
    print(f"   Duplicadas: {stats['duplicados']:,}")
    print(f"   Errores: {stats['errores']:,}")

    return stats


def _insert_batch(session, batch: List[Dict]) -> tuple[int, int]:
    """Inserta batch con ON CONFLICT DO NOTHING."""
    stmt = pg_insert(Concesion).values(batch)
    stmt = stmt.on_conflict_do_nothing(constraint='uq_concesion_id_fecha')

    result = session.execute(stmt)
    inserted = result.rowcount or 0
    duplicated = len(batch) - inserted

    return inserted, duplicated


def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--json', required=True, help='Archivo JSON')
    parser.add_argument('--regimen', required=True,
                       choices=['minimis', 'ayuda_estado', 'partidos_politicos', 'grandes_beneficiarios'])
    parser.add_argument('--batch-size', type=int, default=1000)

    args = parser.parse_args()

    json_path = Path(args.json)
    if not json_path.exists():
        print(f"❌ No existe: {json_path}")
        sys.exit(1)

    stats = load_json_to_concesiones(json_path, args.regimen, args.batch_size)
    sys.exit(0 if stats['errores'] == 0 else 1)


if __name__ == '__main__':
    main()
