# seeding/convocatorias/transform/transform_convocatorias_to_jsonl.py

import json
import sys
import time
import logging
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from sqlalchemy import text
from bdns_core.db.session import get_session


# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


CAMPO_MAP = {
    "codigoBDNS": "codigo_bdns",
    "descripcion": "descripcion",
    "descripcionLeng": "descripcion_leng",
    "fechaRecepcion": "fecha_recepcion",
    "fechaInicioSolicitud": "fecha_inicio_solicitud",
    "fechaFinSolicitud": "fecha_fin_solicitud",
    "sedeElectronica": "sede_electronica",
    "presupuestoTotal": "presupuesto_total",
    "mrr": "mrr",
    "tipoConvocatoria": "tipo_convocatoria",
    "descripcionBasesReguladoras": "descripcion_bases_reguladoras",
    "urlBasesReguladoras": "url_bases_reguladoras",
    "sePublicaDiarioOficial": "se_publica_diario_oficial",
    "abierto": "abierto",
    "ayudaEstado": "ayuda_estado",
    "urlAyudaEstado": "url_ayuda_estado",
}


def parse_fecha(fecha_str: Optional[str]) -> Optional[str]:
    """Normaliza fechas a formato ISO."""
    if not fecha_str:
        return None
    try:
        return fecha_str  # API ya devuelve YYYY-MM-DD
    except (ValueError, TypeError):
        logger.warning(f"Fecha inválida: {fecha_str}")
        return None


def resolver_fk_por_descripcion(session, tabla: str, descripcion: Optional[str]) -> Optional[str]:
    """Resuelve UUID de catálogo buscando por descripcion_norm."""
    if not descripcion:
        return None
    
    from unicodedata import normalize
    descripcion_norm = normalize('NFKD', descripcion.lower())
    descripcion_norm = ''.join(c for c in descripcion_norm if not c.iscombining())
    
    result = session.execute(
        text(f"SELECT id FROM bdns.{tabla} WHERE descripcion_norm = :desc"),
        {"desc": descripcion_norm}
    ).scalar()
    
    if result:
        logger.debug(f"Resuelto {tabla}: '{descripcion[:30]}...' -> {result}")
    else:
        logger.warning(f"No se encontró {tabla} para: '{descripcion[:50]}...'")
    
    return str(result) if result else None


def resolver_organo_id(session, organo_data: Optional[Dict]) -> Optional[str]:
    """Resuelve UUID de órgano por niveles jerárquicos."""
    if not organo_data:
        return None
    
    nivel1 = organo_data.get("nivel1", "")
    nivel2 = organo_data.get("nivel2", "")
    nivel3 = organo_data.get("nivel3")
    
    query = """
        SELECT id FROM bdns.organo 
        WHERE nivel1_norm = :n1 AND nivel2_norm = :n2
    """
    params = {
        "n1": nivel1.lower() if nivel1 else "",
        "n2": nivel2.lower() if nivel2 else ""
    }
    
    if nivel3:
        query += " AND nivel3_norm = :n3"
        params["n3"] = nivel3.lower()
    else:
        query += " AND nivel3_norm IS NULL"
    
    result = session.execute(text(query), params).scalar()
    
    if not result:
        logger.warning(f"Órgano no encontrado: {nivel1}/{nivel2}/{nivel3}")
    
    return str(result) if result else None


def extraer_ids_array(session, tabla: str, items: Optional[List[Dict]], campo_descripcion: str = "descripcion") -> List[str]:
    """Resuelve UUIDs para array de objetos."""
    if not items:
        return []
    
    ids = []
    for item in items:
        desc = item.get(campo_descripcion)
        fk_id = resolver_fk_por_descripcion(session, tabla, desc)
        if fk_id:
            ids.append(fk_id)
    
    logger.debug(f"Resueltos {len(ids)}/{len(items)} {tabla}")
    return ids


def extraer_sector_actividad_ids(session, sectores: Optional[List[Dict]]) -> List[str]:
    """Sectores tienen código y descripción."""
    if not sectores:
        return []
    
    ids = []
    for sector in sectores:
        codigo = sector.get("codigo")
        descripcion = sector.get("descripcion")
        
        if codigo:
            result = session.execute(
                text("SELECT id FROM bdns.sector_actividad WHERE codigo = :cod"),
                {"cod": codigo}
            ).scalar()
            if result:
                ids.append(str(result))
                continue
        
        fk_id = resolver_fk_por_descripcion(session, "sector_actividad", descripcion)
        if fk_id:
            ids.append(fk_id)
    
    return ids


def extraer_tipo_beneficiario_ids(session, tipos: Optional[List[Dict]]) -> List[str]:
    """Tipos de beneficiario con código o descripción."""
    if not tipos:
        return []
    
    ids = []
    for tipo in tipos:
        codigo = tipo.get("codigo")
        descripcion = tipo.get("descripcion")
        
        if codigo:
            result = session.execute(
                text("SELECT id FROM bdns.tipo_beneficiario WHERE codigo = :cod"),
                {"cod": codigo}
            ).scalar()
            if result:
                ids.append(str(result))
                continue
        
        fk_id = resolver_fk_por_descripcion(session, "tipo_beneficiario", descripcion)
        if fk_id:
            ids.append(fk_id)
    
    return ids


def transformar_convocatoria(session, raw: Dict) -> Optional[Dict]:
    """Transforma un objeto convocatoria de la API a nuestro esquema."""
    codigo_bdns = raw.get("codigoBDNS")
    if not codigo_bdns:
        logger.warning("Convocatoria sin código BDNS, saltando")
        return None
    
    result = {"codigo_bdns": codigo_bdns}
    
    for api_field, our_field in CAMPO_MAP.items():
        if api_field == "codigoBDNS":
            continue
        result[our_field] = raw.get(api_field)
    
    # Normalizar fechas
    for campo_fecha in ["fecha_recepcion", "fecha_inicio_solicitud", "fecha_fin_solicitud"]:
        if result.get(campo_fecha):
            result[campo_fecha] = parse_fecha(result[campo_fecha])
    
    # Resolver FKs
    logger.debug(f"Resolviendo FKs para convocatoria {codigo_bdns}")
    
    result["organo_id"] = resolver_organo_id(session, raw.get("organo"))
    result["reglamento_id"] = resolver_fk_por_descripcion(session, "reglamento", 
                                                          raw.get("reglamento", {}).get("descripcion") if raw.get("reglamento") else None)
    result["finalidad_id"] = resolver_fk_por_descripcion(session, "finalidad", raw.get("descripcionFinalidad"))
    
    # Arrays N:M
    result["instrumento_ids"] = extraer_ids_array(session, "instrumento", raw.get("instrumentos"))
    result["tipo_beneficiario_ids"] = extraer_tipo_beneficiario_ids(session, raw.get("tiposBeneficiarios"))
    result["sector_actividad_ids"] = extraer_sector_actividad_ids(session, raw.get("sectores"))
    result["region_ids"] = extraer_ids_array(session, "region", raw.get("regiones"))
    result["fondo_ids"] = extraer_ids_array(session, "fondo", raw.get("fondos"))
    result["objetivo_ids"] = extraer_ids_array(session, "objetivo", raw.get("objetivos"))
    result["sector_producto_ids"] = extraer_ids_array(session, "sector_producto", raw.get("sectoresProductos"))
    
    # Entidades anidadas (para carga posterior)
    result["documentos"] = raw.get("documentos", [])
    result["anuncios"] = raw.get("anuncios", [])
    
    return result


def transform_convocatorias_jsonl(input_json: Path, output_jsonl: Path) -> int:
    """Lee JSON crudo de la API, transforma y escribe JSONL normalizado."""
    logger.info(f"Iniciando transformación: {input_json.name} -> {output_jsonl.name}")
    
    with open(input_json, "r", encoding="utf-8") as f:
        raw_data = json.load(f)
    
    if isinstance(raw_data, dict):
        raw_data = [raw_data]
    
    logger.info(f"Leídas {len(raw_data)} convocatorias del JSON de entrada")
    
    with get_session() as session:
        count = 0
        skipped = 0
        with open(output_jsonl, "w", encoding="utf-8") as fout:
            for i, raw_conv in enumerate(raw_data, 1):
                try:
                    transformed = transformar_convocatoria(session, raw_conv)
                    if transformed:
                        fout.write(json.dumps(transformed, ensure_ascii=False, separators=(",", ":")))
                        fout.write("\n")
                        count += 1
                    else:
                        skipped += 1
                except Exception as e:
                    logger.error(f"Error transformando convocatoria {i}: {e}", exc_info=True)
                    skipped += 1
        
        logger.info(f"Transformación completada: {count} convocatorias escritas, {skipped} omitidas")
    
    return count


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Transforma JSON crudo de API a JSONL normalizado")
    parser.add_argument("--input", required=True, help="JSON de entrada (crudo de API)")
    parser.add_argument("--output", required=True, help="JSONL de salida (normalizado)")
    parser.add_argument("--verbose", "-v", action="store_true", help="Nivel DEBUG")
    args = parser.parse_args()
    
    if args.verbose:
        logger.setLevel(logging.DEBUG)
        logger.debug("Modo verbose activado")
    
    try:
        total = transform_convocatorias_jsonl(Path(args.input), Path(args.output))
        logger.info(f"ETL completado exitosamente")
        print(f"{total}", file=sys.stderr)  # Para scripting
    except Exception as e:
        logger.critical(f"ETL falló: {e}", exc_info=True)
        sys.exit(1)