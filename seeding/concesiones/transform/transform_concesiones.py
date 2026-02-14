#!/usr/bin/env python3
"""
TRANSFORM: Lee los 4 JSONL crudos (ordinarias, minimis, ayudas_estado, partidos_politicos),
resuelve FKs contra BD, separa beneficiarios pendientes y genera JSONL unificado
listo para carga con prioridad de enriquecimiento.

Entrada:  seeding/concesiones/data/jsonl/concesiones_*.jsonl
Salida:   seeding/concesiones/data/jsonl/transformed/concesiones_{year}.jsonl
          seeding/concesiones/data/jsonl/transformed/beneficiarios_pendientes_{year}.jsonl
"""

import json
import logging
import argparse
from pathlib import Path
from datetime import datetime

from bdns_core.db.session import get_session
from bdns_core.db.models import Beneficiario, Convocatoria, Instrumento, RegimenAyuda, Organo
from bdns_core.db.utils import normalizar

MODULO = "transform_concesiones"

# Rutas
DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "jsonl"
TRANSFORMED_DIR = DATA_DIR / "transformed"
TRANSFORMED_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


def log(msg):
    logger.info(f"[{MODULO}] {msg}")


def cargar_lookups(session) -> dict:
    """Carga lookups de BD para resolver FKs."""
    # Convocatorias: codigo_bdns (str) -> UUID
    convocatorias = {}
    for row in session.query(Convocatoria.codigo_bdns, Convocatoria.id).all():
        if row[0]:
            convocatorias[str(row[0])] = str(row[1])

    # Beneficiarios: nif -> UUID
    beneficiarios = {}
    for row in session.query(Beneficiario.nif, Beneficiario.id).all():
        if row[0]:
            beneficiarios[row[0]] = str(row[1])

    # Instrumentos: descripcion_norm -> UUID
    instrumentos = {}
    for row in session.query(Instrumento.descripcion, Instrumento.id).all():
        if row[0]:
            instrumentos[normalizar(row[0])] = str(row[1])

    # Regimenes: descripcion_norm -> UUID
    regimenes = {}
    for row in session.query(RegimenAyuda.descripcion_norm, RegimenAyuda.id).all():
        if row[0]:
            regimenes[row[0]] = str(row[1])

    # Organos: descripcion_norm -> UUID
    organos = {}
    for row in session.query(Organo.descripcion, Organo.id).all():
        if row[0]:
            organos[normalizar(row[0])] = str(row[1])

    return {
        "convocatorias": convocatorias,
        "beneficiarios": beneficiarios,
        "instrumentos": instrumentos,
        "regimenes": regimenes,
        "organos": organos,
    }


def resolver_organo_id(record: dict, organos: dict) -> str | None:
    """Intenta resolver el organo_id desde los campos del registro."""
    # Intentar por niveles (ordinarias y partidos_politicos)
    for campo in ["organo_nivel3", "organo_nivel2", "organo_nivel1"]:
        valor = record.get(campo)
        if valor:
            norm = normalizar(valor)
            if norm in organos:
                return organos[norm]

    # Intentar por organo_convocante (minimis y ayudas_estado)
    organo_conv = record.get("organo_convocante")
    if organo_conv:
        norm = normalizar(organo_conv)
        if norm in organos:
            return organos[norm]

    return None


def transformar_registro(record: dict, lookups: dict) -> tuple[dict | None, dict | None]:
    """
    Transforma un registro JSONL crudo a formato listo para carga.

    Returns:
        (registro_transformado, beneficiario_pendiente_o_None)
    """
    meta = record.get("_meta", {})
    id_concesion = record.get("id_concesion")

    if not id_concesion:
        return None, None

    # Resolver convocatoria_id
    codigo_bdns = record.get("codigo_bdns")
    convocatoria_id = lookups["convocatorias"].get(str(codigo_bdns)) if codigo_bdns else None

    # Resolver beneficiario_id
    nif = record.get("beneficiario_nif")
    beneficiario_id = lookups["beneficiarios"].get(nif) if nif else None

    # Preparar beneficiario pendiente si no existe
    beneficiario_pendiente = None
    if nif and not beneficiario_id:
        beneficiario_pendiente = {
            "nif": nif,
            "nombre": record.get("beneficiario_nombre"),
        }

    # Resolver organo_id
    organo_id = resolver_organo_id(record, lookups["organos"])

    # Resolver instrumento_id
    instrumento = record.get("instrumento_descripcion")
    instrumento_id = None
    if instrumento:
        instrumento_id = lookups["instrumentos"].get(normalizar(instrumento))

    # Resolver regimen_ayuda_id
    regimen_desc = record.get("regimen_descripcion")
    regimen_ayuda_id = None
    if regimen_desc:
        regimen_ayuda_id = lookups["regimenes"].get(normalizar(regimen_desc))

    # Parsear fecha
    fecha = record.get("fecha_concesion")
    if fecha and len(str(fecha)) >= 10:
        fecha = str(fecha)[:10]
    else:
        fecha = None

    # Parsear importes
    importe_nominal = None
    try:
        val = record.get("importe_nominal")
        importe_nominal = float(val) if val else None
    except (ValueError, TypeError):
        pass

    importe_equivalente = None
    try:
        val = record.get("importe_equivalente")
        importe_equivalente = float(val) if val else None
    except (ValueError, TypeError):
        pass

    # Parsear booleano
    tiene_proyecto = record.get("tiene_proyecto")
    if isinstance(tiene_proyecto, str):
        tiene_proyecto = tiene_proyecto.lower() in ("true", "1", "s", "si")
    elif not isinstance(tiene_proyecto, bool):
        tiene_proyecto = False

    resultado = {
        "_meta": meta,
        "id_concesion": str(id_concesion),
        "codigo_bdns": str(codigo_bdns) if codigo_bdns else None,
        "convocatoria_id": convocatoria_id,
        "organo_id": organo_id,
        "beneficiario_id": beneficiario_id,
        "instrumento_id": instrumento_id,
        "regimen_ayuda_id": regimen_ayuda_id,
        "regimen_tipo": meta.get("regimen_tipo", "ordinaria"),
        "fecha_concesion": fecha,
        "importe_nominal": importe_nominal,
        "importe_equivalente": importe_equivalente,
        "url_bases_reguladoras": record.get("url_bases_reguladoras"),
        "tiene_proyecto": tiene_proyecto,
        # Campos enriquecidos (pass-through desde el extract)
        "reglamento_descripcion": record.get("reglamento_descripcion"),
        "objetivo_descripcion": record.get("objetivo_descripcion"),
        "tipo_beneficiario": record.get("tipo_beneficiario"),
        "sector_actividad": record.get("sector_actividad"),
        "region": record.get("region"),
        "ayuda_estado_codigo": record.get("ayuda_estado_codigo"),
        "ayuda_estado_url": record.get("ayuda_estado_url"),
        "entidad": record.get("entidad"),
        "intermediario": record.get("intermediario"),
    }

    return resultado, beneficiario_pendiente


def main():
    parser = argparse.ArgumentParser(description=f"{MODULO}: Transforma JSONL crudos a formato de carga.")
    parser.add_argument("--year", type=int, required=True, help="Ejercicio a procesar")
    args = parser.parse_args()
    year = args.year

    log(f"Transformando concesiones del ejercicio {year}")

    # Buscar JSONL de entrada
    jsonl_files = sorted(DATA_DIR.glob(f"concesiones_*_{year}.jsonl"))
    if not jsonl_files:
        log(f"No se encontraron JSONL en {DATA_DIR} para {year}")
        return 1

    log(f"Encontrados {len(jsonl_files)} JSONL: {[f.name for f in jsonl_files]}")

    # Cargar lookups de BD
    log("Cargando lookups de BD...")
    with get_session() as session:
        lookups = cargar_lookups(session)

    log(f"Lookups: {len(lookups['convocatorias'])} convocatorias, "
        f"{len(lookups['beneficiarios'])} beneficiarios, "
        f"{len(lookups['instrumentos'])} instrumentos, "
        f"{len(lookups['regimenes'])} regimenes, "
        f"{len(lookups['organos'])} organos")

    # Procesar todos los JSONL
    total_in = 0
    total_out = 0
    total_skipped = 0
    beneficiarios_pendientes = {}  # nif -> {nif, nombre}

    out_path = TRANSFORMED_DIR / f"concesiones_{year}.jsonl"
    with open(out_path, "w", encoding="utf-8") as f_out:
        for jsonl_file in jsonl_files:
            count_in = 0
            count_out = 0
            log(f"Procesando {jsonl_file.name}...")

            with open(jsonl_file, "r", encoding="utf-8") as f_in:
                for line in f_in:
                    line = line.strip()
                    if not line:
                        continue
                    count_in += 1

                    try:
                        record = json.loads(line)
                    except json.JSONDecodeError:
                        total_skipped += 1
                        continue

                    resultado, beneficiario_pend = transformar_registro(record, lookups)

                    if resultado:
                        f_out.write(json.dumps(resultado, ensure_ascii=False) + "\n")
                        count_out += 1

                    if beneficiario_pend and beneficiario_pend["nif"]:
                        beneficiarios_pendientes[beneficiario_pend["nif"]] = beneficiario_pend

            total_in += count_in
            total_out += count_out
            log(f"  {jsonl_file.name}: {count_in} leidos -> {count_out} transformados")

    log(f"Total: {total_in} leidos, {total_out} transformados, {total_skipped} saltados")
    log(f"Salida: {out_path}")

    # Guardar beneficiarios pendientes
    if beneficiarios_pendientes:
        ben_path = TRANSFORMED_DIR / f"beneficiarios_pendientes_{year}.jsonl"
        with open(ben_path, "w", encoding="utf-8") as f:
            for ben in beneficiarios_pendientes.values():
                f.write(json.dumps(ben, ensure_ascii=False) + "\n")
        log(f"Beneficiarios pendientes: {len(beneficiarios_pendientes)} -> {ben_path}")

    return 0


if __name__ == "__main__":
    exit(main())
