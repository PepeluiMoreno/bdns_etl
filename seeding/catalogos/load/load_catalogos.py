# load_catalogos.py
# Adaptado para esquema UUID: PKs son UUID, business keys son api_id/codigo
# Soporta carga desde JSON local (data/populate/*.json) o desde API BDNS
import json
import logging
import requests
import csv
from bdns_core.db.utils import normalizar
from bdns_core.db.models import (
    Fondo, Region, Reglamento,
    SectorActividad, RegimenAyuda, FormaJuridica
)

API_BASE = "https://www.infosubvenciones.es/bdnstrans/api"
VPD = "GE"
logger = logging.getLogger(__name__)


def _load_json(json_path):
    """Lee datos desde un archivo JSON local."""
    with open(json_path, encoding="utf-8") as f:
        return json.load(f)


def load_catalogo(session, Model, endpoint_or_json, extra_params=None):
    """Carga un catalogo. Si endpoint_or_json es un path .json, lee local; si no, llama a la API."""
    try:
        if str(endpoint_or_json).endswith(".json"):
            data = _load_json(endpoint_or_json)
        else:
            url = f"{API_BASE}/{endpoint_or_json}"
            if extra_params:
                url += "?" + "&".join([f"{k}={v}" for k, v in extra_params.items()])
            r = requests.get(url, timeout=30)
            r.raise_for_status()
            data = r.json()

        for item in data:
            existente = session.query(Model).filter(Model.api_id == item["id"]).one_or_none()
            if existente:
                existente.descripcion = item["descripcion"]
                existente.descripcion_norm = normalizar(item["descripcion"])
            else:
                obj = Model(
                    api_id=item["id"],
                    descripcion=item["descripcion"],
                    descripcion_norm=normalizar(item["descripcion"]),
                )
                session.add(obj)
        session.commit()
        logger.info(f"{Model.__name__}: {len(data)} registros insertados/actualizados.")
    except Exception as e:
        logger.exception(f"Error al poblar {Model.__name__}: {e}")


def load_regiones(session, json_path=None):
    """Carga regiones jerarquicas. Usa api_id como business key."""
    _api_uuid = {}

    def insertar_region(item, id_padre_uuid=None):
        existente = session.query(Region).filter(Region.api_id == item["id"]).one_or_none()
        if existente:
            _api_uuid[item["id"]] = existente.id
        else:
            region = Region(
                api_id=item["id"],
                descripcion=item["descripcion"],
                descripcion_norm=normalizar(item["descripcion"]),
                id_padre=id_padre_uuid,
            )
            session.add(region)
            session.flush()
            _api_uuid[item["id"]] = region.id

        parent_uuid = _api_uuid[item["id"]]
        for hijo in item.get("children", []):
            insertar_region(hijo, id_padre_uuid=parent_uuid)

    try:
        if json_path:
            data = _load_json(json_path)
        else:
            r = requests.get(f"{API_BASE}/regiones", timeout=30)
            r.raise_for_status()
            data = r.json()

        for item in data:
            insertar_region(item)
        session.commit()
        logger.info("Regiones insertadas/actualizadas.")
    except Exception as e:
        logger.exception("Error al poblar regiones: %s", e)


def load_sector_actividad_desde_csv(session, ruta_csv):
    """Carga sectores CNAE desde CSV. Usa codigo como business key."""
    _codigo_uuid = {}

    try:
        with open(ruta_csv, encoding="utf-8") as f:
            reader = csv.DictReader(f, delimiter=":")
            items = list(reader)

        nodos = {}
        for row in items:
            codigo = row["CODINTEGR"].strip()
            descripcion = row["TITULO_CNAE2009"].strip()
            norm = normalizar(descripcion)
            if codigo not in nodos:
                nodos[codigo] = {
                    "codigo": codigo,
                    "descripcion": descripcion,
                    "descripcion_norm": norm,
                }

        for codigo in sorted(nodos.keys(), key=len):
            datos = nodos[codigo]
            existente = session.query(SectorActividad).filter(
                SectorActividad.codigo == codigo
            ).one_or_none()

            if existente:
                _codigo_uuid[codigo] = existente.id
                continue

            padre_codigo = None
            if len(codigo) == 3:
                padre_codigo = codigo[0]
            elif len(codigo) == 4:
                padre_codigo = codigo[:3]
            elif len(codigo) == 5:
                padre_codigo = codigo[:4]

            id_padre = _codigo_uuid.get(padre_codigo) if padre_codigo else None

            sector = SectorActividad(
                codigo=codigo,
                descripcion=datos["descripcion"],
                descripcion_norm=datos["descripcion_norm"],
                id_padre=id_padre,
            )
            session.add(sector)
            session.flush()
            _codigo_uuid[codigo] = sector.id

        session.commit()
        logger.info("Sectores de actividad insertados desde CSV.")
    except Exception as e:
        logger.exception(f"Error al poblar sectores de actividad desde CSV: {e}")


def load_fondo_desde_csv(session, ruta_csv):
    """Carga fondos europeos desde CSV. Usa api_id como business key."""
    try:
        with open(ruta_csv, encoding="utf-8") as f:
            reader = csv.DictReader(f, delimiter=";")
            items = list(reader)
        for row in items:
            api_id = int(row["id"].strip())
            descripcion = row["descripcion"].strip()
            descripcion_norm = normalizar(descripcion)

            existente = session.query(Fondo).filter(Fondo.api_id == api_id).one_or_none()
            if not existente:
                fondo = Fondo(
                    api_id=api_id,
                    descripcion=descripcion,
                    descripcion_norm=descripcion_norm,
                )
                session.add(fondo)
        session.commit()
        logger.info("Fondos insertados desde CSV.")
    except Exception as e:
        logger.exception(f"Error al poblar fondos desde CSV: {e}")


def load_reglamento_desde_json(session, json_paths, csv_path=None):
    """Carga reglamentos desde JSONs locales (uno por ambito) + CSV complementario."""
    AMBITOS = {'C': 'Concesiones', 'A': 'Ayudas de Estado', 'M': 'de Minimis'}
    vistos = set()
    try:
        for ambito, json_path in json_paths.items():
            data = _load_json(json_path)
            for item in data:
                if item["id"] in vistos:
                    continue
                vistos.add(item["id"])
                existente = session.query(Reglamento).filter(
                    Reglamento.api_id == item["id"]
                ).one_or_none()
                if not existente:
                    reglamento = Reglamento(
                        api_id=item["id"],
                        descripcion=item["descripcion"],
                        descripcion_norm=normalizar(item["descripcion"]),
                        ambito=ambito,
                    )
                    session.add(reglamento)
            logger.info(f"Reglamentos ({AMBITOS[ambito]}) cargados desde JSON.")
        session.commit()
    except Exception as e:
        logger.exception(f"Error al poblar Reglamentos desde JSON: {e}")

    if csv_path:
        try:
            with open(csv_path, encoding="utf-8") as f:
                reader = csv.DictReader(f, delimiter=";")
                for row in reader:
                    api_id = int(row["id"].strip())
                    existente = session.query(Reglamento).filter(
                        Reglamento.api_id == api_id
                    ).one_or_none()
                    if not existente:
                        reglamento = Reglamento(
                            api_id=api_id,
                            descripcion=row["descripcion"].strip(),
                            descripcion_norm=normalizar(row["descripcion"].strip()),
                            ambito=row["ambito"].strip(),
                        )
                        session.add(reglamento)
            session.commit()
            logger.info("Reglamentos complementarios insertados desde CSV.")
        except Exception as e:
            logger.exception(f"Error al poblar reglamentos desde CSV: {e}")


def load_regimen_ayuda_desde_csv(session, ruta_csv):
    """Carga regimenes de ayuda desde CSV. Usa descripcion_norm como business key."""
    try:
        with open(ruta_csv, encoding="utf-8") as f:
            reader = csv.DictReader(f, delimiter=";")
            for row in reader:
                descripcion = row["descripcion"].strip()
                descripcion_norm = row["descripcion_norm"].strip()
                existente = session.query(RegimenAyuda).filter(
                    RegimenAyuda.descripcion_norm == descripcion_norm
                ).one_or_none()
                if not existente:
                    regimen = RegimenAyuda(
                        descripcion=descripcion,
                        descripcion_norm=descripcion_norm,
                    )
                    session.add(regimen)
        session.commit()
        logger.info("Regimenes de ayuda insertados desde CSV.")
    except Exception as e:
        logger.exception(f"Error al poblar regimenes de ayuda desde CSV: {e}")


def load_forma_juridica_desde_csv(session, ruta_csv):
    """Carga formas juridicas desde CSV. Usa codigo como business key."""
    try:
        with open(ruta_csv, encoding="utf-8") as f:
            reader = csv.DictReader(f, delimiter=";")
            for row in reader:
                codigo = row["codigo"].strip()
                codigo_natural = row["codigo_natural"].strip()
                descripcion = row["descripcion"].strip()
                descripcion_norm = row["descripcion_norm"].strip()
                tipo = row["tipo"].strip()
                es_persona_fisica = bool(int(row["es_persona_fisica"].strip()))

                existente = session.query(FormaJuridica).filter(
                    FormaJuridica.codigo == codigo
                ).one_or_none()
                if not existente:
                    forma = FormaJuridica(
                        codigo=codigo,
                        codigo_natural=codigo_natural,
                        descripcion=descripcion,
                        descripcion_norm=descripcion_norm,
                        tipo=tipo,
                        es_persona_fisica=es_persona_fisica,
                    )
                    session.add(forma)
        session.commit()
        logger.info("Formas juridicas insertadas desde CSV.")
    except Exception as e:
        logger.exception(f"Error al poblar formas juridicas desde CSV: {e}")
