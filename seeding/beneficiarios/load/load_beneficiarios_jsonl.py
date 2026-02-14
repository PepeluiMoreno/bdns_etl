#!/usr/bin/env python3
"""
LOAD: Carga beneficiarios desde JSONL usando COPY nativo de PostgreSQL.
"""

import subprocess
import re
import logging
import argparse
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from bdns_core.db.session import get_database_url

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


def generate_load_sql(jsonl_path: Path) -> str:
    """Genera script SQL para cargar beneficiarios."""
    return f"""
\\set ON_ERROR_STOP on

-- Tabla temporal
DROP TABLE IF EXISTS temp_beneficiarios;
CREATE TEMP TABLE temp_beneficiarios (
    id_beneficiario_api BIGINT,
    nombre VARCHAR(500)
);

-- COPY desde JSONL (extraer campos del JSON)
COPY temp_beneficiarios (id_beneficiario_api, nombre)
FROM PROGRAM 'cat {jsonl_path.absolute()} | jq -r ''[.id_beneficiario_api, .nombre] | @tsv'' '
WITH (FORMAT text, DELIMITER E'\\t');

-- Insertar beneficiarios nuevos con UUID generado por PostgreSQL
INSERT INTO bdns.beneficiario (
    id, nif, nombre, nombre_norm, forma_juridica_id, tipo_beneficiario_id, created_at, created_by
)
SELECT 
    uuid_generate_v7(),
    tb.id_beneficiario_api::TEXT,  -- Guardamos el ID de API en nif temporalmente
    tb.nombre,
    lower(unaccent(tb.nombre)),
    NULL,  -- forma_juridica_id desconocida
    NULL,  -- tipo_beneficiario_id desconocido
    NOW(),
    'etl_loader'
FROM temp_beneficiarios tb
WHERE NOT EXISTS (
    SELECT 1 FROM bdns.beneficiario b 
    WHERE b.nif = tb.id_beneficiario_api::TEXT
);

SELECT COUNT(*) as insertados FROM bdns.beneficiario WHERE created_by = 'etl_loader' AND created_at > NOW() - INTERVAL '1 minute';
"""


def load_beneficiarios_jsonl(jsonl_path: Path) -> int:
    """Carga beneficiarios via psql COPY."""
    if not jsonl_path.exists():
        logger.error(f"JSONL no encontrado: {jsonl_path}")
        raise FileNotFoundError(jsonl_path)
    
    db_url = get_database_url()
    sql = generate_load_sql(jsonl_path)
    
    logger.info(f"Cargando beneficiarios desde {jsonl_path.name}")
    
    result = subprocess.run(
        ["psql", db_url, "-v", "ON_ERROR_STOP=1", "-c", sql],
        capture_output=True,
        text=True
    )
    
    if result.returncode != 0:
        logger.error(f"psql falló: {result.stderr}")
        raise RuntimeError(result.stderr)
    
    # Parsear conteo
    match = re.search(r'insertados\s*\n\s*(\d+)', result.stdout + result.stderr)
    insertados = int(match.group(1)) if match else 0
    
    logger.info(f"Beneficiarios insertados: {insertados}")
    return insertados


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Carga beneficiarios desde JSONL")
    parser.add_argument("--jsonl", required=True, help="Archivo JSONL de beneficiarios")
    args = parser.parse_args()
    
    load_beneficiarios_jsonl(Path(args.jsonl))