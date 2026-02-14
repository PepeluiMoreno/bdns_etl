#!/usr/bin/env python3
"""
LOAD: Carga concesiones desde JSONL usando COPY nativo + INSERT/UPDATE con prioridad.
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
    """Genera script SQL completo para cargar concesiones con lógica de prioridad."""
    return f"""
\\set ON_ERROR_STOP on

-- ============================================
-- 1. TABLA TEMPORAL CON JSONB
-- ============================================
DROP TABLE IF EXISTS temp_concesiones_json;
CREATE TEMP TABLE temp_concesiones_json (
    data JSONB NOT NULL
);

-- COPY del JSONL línea por línea
COPY temp_concesiones_json (data)
FROM '{jsonl_path.absolute()}'
WITH (FORMAT text);

-- ============================================
-- 2. CREAR TABLA TEMPORAL DESNORMALIZADA
-- ============================================
DROP TABLE IF EXISTS temp_concesiones;
CREATE TEMP TABLE temp_concesiones AS
SELECT 
    data->>'id_concesion' as id_concesion,
    data->>'codigo_bdns' as codigo_bdns,
    NULLIF(data->>'convocatoria_id', '')::UUID as convocatoria_id,
    NULLIF(data->>'organo_id', '')::UUID as organo_id,
    NULLIF(data->>'beneficiario_id', '')::UUID as beneficiario_id,
    NULLIF(data->>'instrumento_id', '')::UUID as instrumento_id,
    NULLIF(data->>'regimen_ayuda_id', '')::UUID as regimen_ayuda_id,
    data->>'regimen_tipo' as regimen_tipo,
    NULLIF(data->>'fecha_concesion', '')::DATE as fecha_concesion,
    NULLIF(data->>'importe_nominal', '')::FLOAT as importe_nominal,
    NULLIF(data->>'importe_equivalente', '')::FLOAT as importe_equivalente,
    data->>'url_bases_reguladoras' as url_bases_reguladoras,
    COALESCE((data->>'tiene_proyecto')::BOOLEAN, false) as tiene_proyecto,
    -- Campos enriquecidos
    data->>'reglamento_descripcion' as reglamento_descripcion,
    data->>'objetivo_descripcion' as objetivo_descripcion,
    data->>'tipo_beneficiario' as tipo_beneficiario,
    data->>'sector_actividad' as sector_actividad,
    data->>'region' as region,
    data->>'ayuda_estado_codigo' as ayuda_estado_codigo,
    data->>'ayuda_estado_url' as ayuda_estado_url,
    data->>'entidad' as entidad,
    data->>'intermediario' as intermediario,
    -- Prioridad para lógica de enriquecimiento
    (data->'_meta'->>'prioridad')::INTEGER as prioridad
FROM temp_concesiones_json;

CREATE INDEX idx_temp_concesiones_id ON temp_concesiones(id_concesion);
CREATE INDEX idx_temp_concesiones_prioridad ON temp_concesiones(id_concesion, prioridad DESC);

-- ============================================
-- 3. INSERT DE NUEVAS CONCESIONES (no existen)
-- ============================================
INSERT INTO bdns.concesion (
    id, id_concesion, convocatoria_id, beneficiario_id, organo_id,
    instrumento_id, regimen_ayuda_id, regimen_tipo,
    fecha_concesion, importe_nominal, importe_equivalente,
    url_bases_reguladoras, tiene_proyecto,
    reglamento_descripcion, objetivo_descripcion, tipo_beneficiario,
    sector_actividad, region, ayuda_estado_codigo, ayuda_estado_url,
    entidad, intermediario,
    created_at, created_by
)
SELECT 
    uuid_generate_v7(),
    t.id_concesion,
    t.convocatoria_id,
    t.beneficiario_id,
    t.organo_id,
    t.instrumento_id,
    t.regimen_ayuda_id,
    t.regimen_tipo,
    t.fecha_concesion,
    t.importe_nominal,
    t.importe_equivalente,
    t.url_bases_reguladoras,
    t.tiene_proyecto,
    t.reglamento_descripcion,
    t.objetivo_descripcion,
    t.tipo_beneficiario,
    t.sector_actividad,
    t.region,
    t.ayuda_estado_codigo,
    t.ayuda_estado_url,
    t.entidad,
    t.intermediario,
    NOW(),
    'etl_loader'
FROM temp_concesiones t
WHERE NOT EXISTS (
    SELECT 1 FROM bdns.concesion c WHERE c.id_concesion = t.id_concesion
);

-- Contar insertadas
SELECT COUNT(*) as insertadas FROM bdns.concesion WHERE created_by = 'etl_loader' AND created_at > NOW() - INTERVAL '1 minute';

-- ============================================
-- 4. UPDATE DE EXISTENTES (enriquecimiento con mayor prioridad)
-- ============================================
-- Solo actualizar si la nueva tiene mayor prioridad que la existente
UPDATE bdns.concesion c
SET 
    regimen_ayuda_id = t.regimen_ayuda_id,
    regimen_tipo = t.regimen_tipo,
    -- Enriquecer campos que no teníamos
    reglamento_descripcion = COALESCE(t.reglamento_descripcion, c.reglamento_descripcion),
    objetivo_descripcion = COALESCE(t.objetivo_descripcion, c.objetivo_descripcion),
    tipo_beneficiario = COALESCE(t.tipo_beneficiario, c.tipo_beneficiario),
    sector_actividad = COALESCE(t.sector_actividad, c.sector_actividad),
    region = COALESCE(t.region, c.region),
    ayuda_estado_codigo = COALESCE(t.ayuda_estado_codigo, c.ayuda_estado_codigo),
    ayuda_estado_url = COALESCE(t.ayuda_estado_url, c.ayuda_estado_url),
    entidad = COALESCE(t.entidad, c.entidad),
    intermediario = COALESCE(t.intermediario, c.intermediario),
    -- Actualizar si los nuevos son más específicos
    organo_id = COALESCE(t.organo_id, c.organo_id),
    instrumento_id = COALESCE(t.instrumento_id, c.instrumento_id),
    updated_at = NOW()
FROM temp_concesiones t
WHERE c.id_concesion = t.id_concesion
  AND t.prioridad > CASE c.regimen_tipo
    WHEN 'minimis' THEN 4
    WHEN 'ayudas_estado' THEN 3
    WHEN 'partidos_politicos' THEN 2
    WHEN 'ordinaria' THEN 1
    ELSE 0
  END;

-- Contar actualizadas
SELECT COUNT(*) as actualizadas FROM bdns.concesion WHERE updated_at > NOW() - INTERVAL '1 minute' AND created_by = 'etl_loader';

-- ============================================
-- 5. ESTADÍSTICAS FINALES
-- ============================================
SELECT 
    (SELECT COUNT(*) FROM temp_concesiones) as total_en_jsonl,
    (SELECT COUNT(*) FROM bdns.concesion WHERE created_by = 'etl_loader' AND created_at > NOW() - INTERVAL '10 seconds') as total_insertadas,
    (SELECT COUNT(DISTINCT id_concesion) FROM temp_concesiones) as concesiones_unicas;
"""


def load_concesiones_jsonl(jsonl_path: Path) -> dict:
    """Carga concesiones via psql COPY con INSERT/UPDATE."""
    if not jsonl_path.exists():
        logger.error(f"JSONL no encontrado: {jsonl_path}")
        raise FileNotFoundError(jsonl_path)
    
    db_url = get_database_url()
    sql = generate_load_sql(jsonl_path)
    
    logger.info(f"Cargando concesiones desde {jsonl_path.name}")
    
    result = subprocess.run(
        ["psql", db_url, "-v", "ON_ERROR_STOP=1", "-c", sql],
        capture_output=True,
        text=True
    )
    
    if result.returncode != 0:
        logger.error(f"psql falló: {result.stderr}")
        raise RuntimeError(result.stderr)
    
    # Parsear estadísticas
    output = result.stdout + result.stderr
    
    stats = {}
    patterns = {
        "insertadas": r'insertadas\s*\n\s*(\d+)',
        "actualizadas": r'actualizadas\s*\n\s*(\d+)',
        "total_en_jsonl": r'total_en_jsonl\s*\n\s*(\d+)',
        "concesiones_unicas": r'concesiones_unicas\s*\n\s*(\d+)'
    }
    
    for key, pattern in patterns.items():
        match = re.search(pattern, output)
        stats[key] = int(match.group(1)) if match else 0
    
    logger.info(f"Carga completada:")
    logger.info(f"  - En JSONL: {stats.get('total_en_jsonl', 0)}")
    logger.info(f"  - Insertadas: {stats.get('insertadas', 0)}")
    logger.info(f"  - Actualizadas: {stats.get('actualizadas', 0)}")
    
    return stats


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Carga concesiones desde JSONL con COPY nativo")
    parser.add_argument("--jsonl", required=True, help="Archivo JSONL de concesiones transformadas")
    args = parser.parse_args()
    
    load_concesiones_jsonl(Path(args.jsonl))