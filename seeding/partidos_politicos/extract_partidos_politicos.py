#!/usr/bin/env python3
"""
Extract: Concesiones a partidos políticos desde /partidospoliticos/busqueda
Genera JSONL con campos crudos + metadata de origen.
"""

import json
import logging
import argparse
from datetime import datetime
from pathlib import Path

import requests
import sys

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

PAGE_SIZE = 10000
URL = "https://www.infosubvenciones.es/bdnstrans/api/partidospoliticos/busqueda"
RUTA_RAW = Path(__file__).resolve().parent / "data" / "jsonl"
RUTA_RAW.mkdir(parents=True, exist_ok=True)


def extract_partidos_politicos(year: int) -> Path:
    """Extrae concesiones a partidos políticos y genera JSONL."""
    desde = f"01/01/{year}"
    hasta = f"31/12/{year}"
    
    output_path = RUTA_RAW / f"concesiones_partidospoliticos_{year}.jsonl"
    
    logger.info(f"Iniciando extracción partidos políticos {year}")
    
    page = 0
    total = 0
    seen_ids = set()
    
    with open(output_path, "w", encoding="utf-8") as fout:
        while True:
            params = {
                "page": page,
                "pageSize": PAGE_SIZE,
                "fechaDesde": desde,
                "fechaHasta": hasta,
            }
            
            try:
                r = requests.get(URL, params=params, timeout=180)
                r.raise_for_status()
                data = r.json()
            except requests.RequestException as e:
                logger.error(f"Error en página {page}: {e}")
                raise
            
            content = data.get("content", [])
            batch_count = 0
            
            for row in content:
                id_concesion = str(row.get("id"))
                
                if id_concesion in seen_ids:
                    logger.warning(f"Concesión {id_concesion} duplicada en extracción partidos políticos")
                    continue
                
                seen_ids.add(id_concesion)
                
                record = {
                    "_meta": {
                        "origen": "partidospoliticos",
                        "regimen_tipo": "partidos_politicos",
                        "fecha_extraccion": datetime.now().isoformat(),
                        "año": year,
                        "pagina": page
                    },
                    "id_concesion": id_concesion,
                    "id_convocatoria_api": row.get("idConvocatoria"),
                    "codigo_bdns": row.get("numeroConvocatoria"),
                    "convocatoria_titulo": row.get("convocatoria"),
                    "organo_nivel1": row.get("nivel1"),
                    "organo_nivel2": row.get("nivel2"),
                    "organo_nivel3": row.get("nivel3"),
                    "organo_convocante": None,
                    "codigo_invente": row.get("codigoINVENTE"),
                    "fecha_concesion": row.get("fechaConcesion"),
                    "fecha_alta_registro": None,
                    "id_beneficiario_api": None,  # No viene en este endpoint
                    "beneficiario_nombre": row.get("beneficiario"),
                    "instrumento_descripcion": row.get("instrumento"),
                    "reglamento_descripcion": None,
                    "objetivo_descripcion": None,
                    "tipo_beneficiario": None,
                    "sector_producto": None,
                    "sector_actividad": None,
                    "region": None,
                    "ayuda_estado_codigo": None,
                    "ayuda_estado_url": None,
                    "entidad": None,
                    "intermediario": None,
                    "importe_nominal": row.get("importe"),
                    "importe_equivalente": row.get("ayudaEquivalente"),
                    "url_bases_reguladoras": row.get("urlBR"),
                    "tiene_proyecto": row.get("tieneProyecto"),
                }
                
                fout.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")))
                fout.write("\n")
                batch_count += 1
            
            total += batch_count
            logger.info(f"Página {page}: {batch_count} registros (total únicos: {total})")
            
            if batch_count < PAGE_SIZE:
                break
            
            page += 1
    
    logger.info(f"Extracción completada: {total} concesiones a partidos políticos")
    return output_path


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extrae concesiones a partidos políticos a JSONL")
    parser.add_argument("--year", type=int, required=True, help="Año a extraer")
    args = parser.parse_args()
    
    extract_partidos_politicos(args.year)