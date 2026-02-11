"""
Transform: convierte JSON raw de convocatorias de la API BDNS a CSV
listo para carga masiva con COPY.

Resuelve FK IDs (organo_id, reglamento_id) contra tablas de catálogos.
Requiere que los catálogos estén poblados al 100%.
"""

import json
import sys
from datetime import datetime
from pathlib import Path
from uuid import uuid4

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from sqlalchemy import text
from bdns_core.db.session import get_session

# Columnas del CSV de salida (deben coincidir con bdns.convocatoria)
CSV_COLUMNS = [
    "id", "id_bdns", "codigo_bdns", "titulo", "descripcion",
    "fecha_recepcion", "fecha_publicacion", "organo_id",
    "presupuesto_total", "reglamento_id", "created_by", "created_at"
]


def _build_organo_lookup(session) -> dict:
    """Construye mapa {codigo_api: uuid} de órganos."""
    rows = session.execute(text("SELECT codigo, id FROM bdns.organo")).all()
    return {str(r[0]): str(r[1]) for r in rows}


def _build_reglamento_lookup(session) -> dict:
    """Construye mapa {api_id: uuid} de reglamentos."""
    rows = session.execute(text("SELECT api_id, id FROM bdns.reglamento WHERE api_id IS NOT NULL")).all()
    return {int(r[0]): str(r[1]) for r in rows}


def _resolve_fk(data, field_name, lookup, key_type=str):
    """Resuelve un FK desde el JSON de la API usando el lookup."""
    value = data.get(field_name)
    if not value:
        return None
    api_id = value.get("id") if isinstance(value, dict) else value
    if api_id is None:
        return None
    try:
        return lookup.get(key_type(api_id))
    except (ValueError, TypeError):
        return None


def _parse_date(date_str):
    """Parsea fecha del API (DD/MM/YYYY o YYYY-MM-DD) a YYYY-MM-DD."""
    if not date_str:
        return ""
    date_str = str(date_str).strip()[:10]
    if "/" in date_str:
        try:
            return datetime.strptime(date_str, "%d/%m/%Y").strftime("%Y-%m-%d")
        except ValueError:
            return ""
    return date_str


def _safe_str(value, max_len=None):
    """Limpia string para CSV compatible con COPY (pipe-separated)."""
    if value is None:
        return "\\N"
    s = str(value).replace("\x00", "").replace("|", " ").strip()
    if max_len:
        s = s[:max_len]
    return s if s else "\\N"


def transform_convocatorias_to_csv(json_path: Path, csv_path: Path) -> dict:
    """
    Transforma JSON raw de convocatorias a CSV para COPY.

    Args:
        json_path: Ruta al JSON con datos crudos de la API
        csv_path: Ruta de salida para el CSV

    Returns:
        Dict con estadísticas: total, transformados, errores, sin_organo, sin_reglamento
    """
    with open(json_path, encoding="utf-8") as f:
        records = json.load(f)

    stats = {
        "total": len(records),
        "transformados": 0,
        "errores": 0,
        "sin_organo": 0,
        "sin_reglamento": 0,
        "sin_id_bdns": 0,
    }

    # Cargar lookups de catálogos
    with get_session() as session:
        organo_map = _build_organo_lookup(session)
        reglamento_map = _build_reglamento_lookup(session)

    print(f"[Transform] Catálogos cargados: {len(organo_map)} órganos, {len(reglamento_map)} reglamentos")
    print(f"[Transform] Procesando {len(records)} convocatorias...")

    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    csv_path.parent.mkdir(parents=True, exist_ok=True)

    with open(csv_path, "w", encoding="utf-8") as f:
        # Header
        f.write("|".join(CSV_COLUMNS) + "\n")

        for rec in records:
            try:
                # ID BDNS (campo obligatorio)
                raw_id = (rec.get("codigo_bdns")
                          or rec.get("numeroConvocatoria")
                          or rec.get("id"))
                if not raw_id:
                    stats["sin_id_bdns"] += 1
                    stats["errores"] += 1
                    continue
                id_bdns = str(raw_id).strip()

                # Resolver FKs
                organo_id = _resolve_fk(rec, "organo", organo_map, str)
                if not organo_id:
                    stats["sin_organo"] += 1

                reglamento_id = _resolve_fk(rec, "reglamento", reglamento_map, int)
                if not reglamento_id:
                    stats["sin_reglamento"] += 1

                row = "|".join([
                    str(uuid4()),                                        # id
                    id_bdns,                                             # id_bdns
                    _safe_str(rec.get("codigo_bdns") or id_bdns),        # codigo_bdns
                    _safe_str(rec.get("descripcion"), 500),              # titulo
                    _safe_str(rec.get("descripcionLeng")
                              or rec.get("descripcion")),                # descripcion
                    _parse_date(rec.get("fechaRecepcion")) or "\\N",     # fecha_recepcion
                    _parse_date(rec.get("fechaPublicacion")) or "\\N",   # fecha_publicacion
                    organo_id or "\\N",                                  # organo_id
                    str(rec.get("presupuestoTotal") or "\\N"),           # presupuesto_total
                    reglamento_id or "\\N",                              # reglamento_id
                    "etl_system",                                        # created_by
                    now,                                                 # created_at
                ])
                f.write(row + "\n")
                stats["transformados"] += 1

            except Exception as e:
                stats["errores"] += 1
                print(f"[Transform] Error en registro: {e}")

    print(f"[Transform] Completado: {stats['transformados']}/{stats['total']} "
          f"({stats['errores']} errores, {stats['sin_organo']} sin órgano, "
          f"{stats['sin_reglamento']} sin reglamento)")

    return stats


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Transforma JSON de convocatorias a CSV")
    parser.add_argument("--json", required=True, help="JSON de entrada")
    parser.add_argument("--csv", required=True, help="CSV de salida")
    args = parser.parse_args()
    transform_convocatorias_to_csv(Path(args.json), Path(args.csv))
