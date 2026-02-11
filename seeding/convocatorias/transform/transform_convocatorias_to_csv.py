"""
Transform: convierte JSON raw de convocatorias de la API BDNS a CSV
listo para carga masiva con COPY.

Resuelve FK IDs (organo_id, reglamento_id) contra tablas de catálogos.
Requiere que los catálogos estén poblados al 100%.

La API de detalle (/api/convocatorias?numConv=X) devuelve:
- organo como dict {nivel1, nivel2, nivel3} (sin código)
- reglamento como dict {descripcion, orden} o null (sin api_id)
- codigoBDNS como string (no codigo_bdns)
"""

import json
import re
import sys
import unicodedata
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


def _normalizar(texto):
    """Normaliza texto para búsqueda: quita tildes, uppercase, colapsa espacios."""
    if not texto:
        return None
    texto = unicodedata.normalize("NFKD", texto).encode("ASCII", "ignore").decode("ASCII")
    texto = texto.upper().strip()
    texto = re.sub(r"\s+", " ", texto)
    return texto


def _build_organo_lookup(session) -> dict:
    """Construye mapa {(n1_norm, n2_norm, n3_norm): uuid} de órganos."""
    rows = session.execute(text(
        "SELECT nivel1_norm, nivel2_norm, nivel3_norm, id FROM bdns.organo "
        "WHERE nivel1_norm IS NOT NULL"
    )).all()
    lookup = {}
    for r in rows:
        key = (r[0] or "", r[1] or "", r[2] or "")
        lookup[key] = str(r[3])
    return lookup


def _build_reglamento_lookup(session) -> dict:
    """Construye mapa {descripcion_norm: uuid} de reglamentos."""
    rows = session.execute(text(
        "SELECT descripcion, descripcion_norm, id FROM bdns.reglamento"
    )).all()
    lookup = {}
    for r in rows:
        if r[1]:
            lookup[r[1]] = str(r[2])
        if r[0]:
            lookup[_normalizar(r[0])] = str(r[2])
    return lookup


def _resolve_organo(organo_dict, lookup):
    """Resuelve organo_id desde el dict {nivel1, nivel2, nivel3} de la API."""
    if not organo_dict or not isinstance(organo_dict, dict):
        return None
    n1 = _normalizar(organo_dict.get("nivel1")) or ""
    n2 = _normalizar(organo_dict.get("nivel2")) or ""
    n3 = _normalizar(organo_dict.get("nivel3")) or ""

    # Buscar con los 3 niveles
    result = lookup.get((n1, n2, n3))
    if result:
        return result
    # Fallback: buscar solo con nivel1 + nivel2 (nivel3 vacío)
    if n3:
        result = lookup.get((n1, n2, ""))
    return result


def _resolve_reglamento(reglamento_dict, lookup):
    """Resuelve reglamento_id desde el dict {descripcion, orden} de la API."""
    if not reglamento_dict or not isinstance(reglamento_dict, dict):
        return None
    desc = reglamento_dict.get("descripcion")
    if not desc:
        return None
    desc_norm = _normalizar(desc)
    # Búsqueda exacta por descripción normalizada
    result = lookup.get(desc_norm)
    if result:
        return result
    # Fallback: buscar si alguna clave del lookup está contenida en la descripción
    for key, uuid in lookup.items():
        if key and key in desc_norm:
            return uuid
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
                # ID BDNS: la API de detalle usa "codigoBDNS" (camelCase)
                raw_id = (rec.get("codigoBDNS")
                          or rec.get("codigo_bdns")
                          or rec.get("numeroConvocatoria")
                          or rec.get("id"))
                if not raw_id:
                    stats["sin_id_bdns"] += 1
                    stats["errores"] += 1
                    continue
                id_bdns = str(raw_id).strip()

                # Resolver FKs usando formato real de la API
                organo_id = _resolve_organo(rec.get("organo"), organo_map)
                if not organo_id:
                    stats["sin_organo"] += 1

                reglamento_id = _resolve_reglamento(rec.get("reglamento"), reglamento_map)
                if not rec.get("reglamento"):
                    pass  # No contar como error si la API no incluye reglamento
                elif not reglamento_id:
                    stats["sin_reglamento"] += 1

                row = "|".join([
                    str(uuid4()),                                        # id
                    id_bdns,                                             # id_bdns
                    _safe_str(id_bdns),                                  # codigo_bdns
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
