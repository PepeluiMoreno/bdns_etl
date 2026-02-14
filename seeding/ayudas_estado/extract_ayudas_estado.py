#!/usr/bin/env python3
"""
Extract: Ayudas de estado desde /ayudasestado/busqueda
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
URL = "https://www.infosubvenciones.es/bdnstrans/api/ayudasestado/busqueda"
RUTA_RAW = Path(__file__).resolve().parent / "data" / "jsonl"
RUTA_RAW.mkdir(parents=True, exist_ok=True)


def extract_ayudas_estado(year: int) -> Path:
    """Extrae ayudas de estado y genera JSONL."""
    desde = f"01/01/{year}"
    hasta = f"31/12/{year}"
    
    output_path = RUTA_RAW / f"concesiones_ayudasestado_{year}.jsonl"
    
    logger.info(f"Iniciando extracción ayudas estado {year}")
    
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
                id_concesion = str(row.get("idConcesion"))
                
                if id_concesion in seen_ids:
                    logger.warning(f"Concesión {id_concesion} duplicada en extracción ayudas estado")
                    continue
                
                seen_ids.add(id_concesion)
                
                record = {
                    "_meta": {
                        "origen": "ayudasestado",
                        "regimen_tipo": "ayudas_estado",
                        "fecha_extraccion": datetime.now().isoformat(),
                        "año": year,
                        "pagina": page
                    },
                    "id_concesion": id_concesion,
                    "id_convocatoria_api": row.get("idConvocatoria"),
                    "codigo_bdns": row.get("numeroConvocatoria"),
                    "convocatoria_titulo": row.get("convocatoria"),
                    "organo_nivel1": None,
                    "organo_nivel2": None,
                    "organo_nivel3": None,
                    "organo_convocante": row.get("convocante"),
                    "codigo_invente": None,
                    "fecha_concesion": row.get("fechaConcesion"),
                    "fecha_alta_registro": row.get("fechaAlta"),
                    "id_beneficiario_api": row.get("idPersona"),
                    "beneficiario_nombre": row.get("beneficiario"),
                    "instrumento_descripcion": row.get("instrumento"),
                    "reglamento_descripcion": row.get("reglamento"),
                    "objetivo_descripcion": row.get("objetivo"),
                    "tipo_beneficiario": row.get("tipoBeneficiario"),
                    "sector_producto": None,
                    "sector_actividad": row.get("sectores"),
                    "region": row.get("region"),
                    "ayuda_estado_codigo": row.get("ayudaEstado"),
                    "ayuda_estado_url": row.get("urlAyudaEstado"),
                    "entidad": row.get("entidad"),
                    "intermediario": row.get("intermediario"),
                    "importe_nominal": row.get("importe"),
                    "importe_equivalente": row.get("ayudaEquivalente"),
                    "url_bases_reguladoras": None,
                    "tiene_proyecto": None,
                }
                
                fout.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")))
                fout.write("\n")
                batch_count += 1
            
            total += batch_count
            logger.info(f"Página {page}: {batch_count} registros (total únicos: {total})")
            
            if batch_count < PAGE_SIZE:
                break
            
            page += 1
    
    logger.info(f"Extracción completada: {total} ayudas de estado")
    return output_path


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extrae ayudas de estado a JSONL")
    parser.add_argument("--year", type=int, required=True, help="Año a extraer")
    args = parser.parse_args()
    
    extract_ayudas_estado(args.year)