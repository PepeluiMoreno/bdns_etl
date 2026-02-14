# seeding/convocatorias/load/load_convocatorias_jsonl.py

import subprocess
import re
import sys
import time
import json
import logging
from pathlib import Path
from typing import Tuple

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from bdns_core.db.session import get_database_url


# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),  # Consola
        # logging.FileHandler("/var/log/bdns/etl.log"),  # Descomentar para fichero
    ]
)
logger = logging.getLogger(__name__)


def generate_load_sql(jsonl_path: Path) -> str:
    """
    Genera script SQL completo para cargar JSONL.
    """
    # ... (igual que antes, omitido por brevedad)
    return f"""..."""


def copy_convocatorias_jsonl(jsonl_path: Path) -> Tuple[int, int, int, int]:
    """
    Ejecuta carga completa via psql.
    Retorna: (total_jsonl, convocatorias, documentos, anuncios)
    """
    if not jsonl_path.exists():
        logger.error(f"JSONL no encontrado: {jsonl_path}")
        raise FileNotFoundError(f"JSONL no encontrado: {jsonl_path}")

    db_url = get_database_url()
    
    logger.info(f"Iniciando carga desde {jsonl_path.name}")
    
    sql_script = generate_load_sql(jsonl_path)
    
    logger.debug(f"Ejecutando psql con script de {len(sql_script)} caracteres")
    
    result = subprocess.run(
        ["psql", db_url, "-v", "ON_ERROR_STOP=1", "-c", sql_script],
        capture_output=True,
        text=True
    )
    
    if result.returncode != 0:
        logger.error(f"psql falló: {result.stderr}")
        raise RuntimeError(f"psql falló: {result.stderr}")
    
    logger.debug("psql ejecutado correctamente")
    
    # Parsear estadísticas
    output = result.stdout + result.stderr
    
    stats = {
        "total_jsonl": 0,
        "convocatorias": 0,
        "documentos": 0,
        "anuncios": 0
    }
    
    patterns = {
        "total_jsonl": r'total_jsonl\s*\n\s*(\d+)',
        "convocatorias": r'convocatorias_insertadas\s*\n\s*(\d+)',
        "documentos": r'documentos_insertados\s*\n\s*(\d+)',
        "anuncios": r'anuncios_insertados\s*\n\s*(\d+)'
    }
    
    for key, pattern in patterns.items():
        match = re.search(pattern, output)
        if match:
            stats[key] = int(match.group(1))
    
    logger.info(f"Carga completada: {stats['convocatorias']} convocatorias, "
                f"{stats['documentos']} documentos, {stats['anuncios']} anuncios "
                f"(de {stats['total_jsonl']} en JSONL)")
    
    if stats['convocatorias'] == 0 and stats['total_jsonl'] > 0:
        logger.warning("No se insertaron convocatorias aunque el JSONL tenía registros")
    
    return (
        stats["total_jsonl"],
        stats["convocatorias"],
        stats["documentos"],
        stats["anuncios"]
    )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Carga JSONL de convocatorias via psql COPY")
    parser.add_argument("--jsonl", required=True, help="Archivo JSONL de entrada")
    parser.add_argument("--verbose", "-v", action="store_true", help="Nivel DEBUG")
    args = parser.parse_args()
    
    if args.verbose:
        logger.setLevel(logging.DEBUG)
        logger.debug("Modo verbose activado")
    
    try:
        total, conv, docs, anunc = copy_convocatorias_jsonl(Path(args.jsonl))
        logger.info(f"ETL completado exitosamente")
        # Salida final para piping/scripting (solo números)
        print(f"{conv},{docs},{anunc},{total}", file=sys.stderr)
    except Exception as e:
        logger.critical(f"ETL falló: {e}", exc_info=True)
        sys.exit(1)