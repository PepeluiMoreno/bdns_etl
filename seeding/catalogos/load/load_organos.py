# load_organos.py
# Adaptado para esquema UUID: Organo.id es UUID, Organo.codigo es business key
import logging
import requests
import json
from pathlib import Path
from bdns_core.db.models import Organo
from bdns_core.db.utils import normalizar
from bdns_core.db.enums import TipoOrganoEnum
from sqlalchemy.exc import SQLAlchemyError

BASE_URL = "https://www.infosubvenciones.es/bdnstrans/api/organos?vpd=GE&idAdmon="

MAPA_PROVINCIAS = {
    "ANDALUCIA": ["ALMERIA", "CADIZ", "CORDOBA", "GRANADA", "HUELVA", "JAEN", "MALAGA", "SEVILLA"],
    "ARAGON": ["HUESCA", "TERUEL", "ZARAGOZA"],
    "CANARIAS": ["PALMAS", "LAS PALMAS", "TENERIFE", "SANTA CRUZ DE TENERIFE"],
    "CANTABRIA": ["CANTABRIA"],
    "CASTILLA Y LEON": ["AVILA", "BURGOS", "LEON", "PALENCIA", "SALAMANCA", "SEGOVIA", "SORIA", "VALLADOLID", "ZAMORA"],
    "CASTILLA-LA MANCHA": ["ALBACETE", "CIUDAD REAL", "CUENCA", "GUADALAJARA", "TOLEDO"],
    "CATALUNA": ["BARCELONA", "GIRONA", "LLEIDA", "TARRAGONA"],
    "CIUDAD AUTONOMA DE MELILLA": ["MELILLA"],
    "COMUNIDAD DE MADRID": ["MADRID"],
    "COMUNIDAD FORAL DE NAVARRA": ["NAFARROA / NAVARRA"],
    "COMUNITAT VALENCIANA": ["ALACANT / ALICANTE",  "CASTELLO / CASTELLON", "VALENCIA / VALENCIA"],
    "CUIDAD AUTONOMA DE CEUTA": ["CEUTA"],
    "EXTREMADURA": ["BADAJOZ", "CACERES"],
    "GALICIA": ["A CORUNA", "CORUNA", "LUGO", "OURENSE", "PONTEVEDRA"],
    "ILLES BALEARS": ["ILLES BALEARS / ISLAS BALEARES"],
    "LA RIOJA": ["LA RIOJA"],
    "PAIS VASCO": ["ARABA / ALAVA", "BIZKAIA", "VIZCAYA", "GIPUZKOA", "GUIPUZCOA"],
    "PRINCIPADO DE ASTURIAS": ["ASTURIAS"],
    "REGION DE MURCIA": ["MURCIA"],
}

logger = logging.getLogger(__name__)

# Cache: codigo -> UUID (se rellena durante la carga)
_codigo_uuid = {}


def _load_json(json_path):
    """Carga datos desde un archivo JSON local."""
    with open(json_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def formar_codigo(tipo: TipoOrganoEnum, id_num: int) -> str:
    return f"{tipo.value}{id_num}"


def generar_codigo_unico(session, codigo_base):
    """Si ya existe un organo con este codigo pero datos diferentes, generar codigo con sufijo."""
    codigo = codigo_base
    contador = 1
    while session.query(Organo).filter(Organo.codigo == codigo).one_or_none():
        codigo = f"{codigo_base}_{contador}"
        contador += 1
    return codigo


def insertar_organo(session, organo_dict):
    """
    Inserta un organo. Usa 'codigo' como business key y 'padre_codigo' para resolver FK.
    El UUID se genera automaticamente.
    """
    codigo = organo_dict.pop('codigo')
    padre_codigo = organo_dict.pop('padre_codigo', None)

    # Resolver padre por codigo -> UUID
    id_padre = None
    if padre_codigo:
        id_padre = _codigo_uuid.get(padre_codigo)
        if not id_padre:
            padre = session.query(Organo).filter(Organo.codigo == padre_codigo).one_or_none()
            if padre:
                id_padre = padre.id
                _codigo_uuid[padre_codigo] = id_padre

    # Verificar si ya existe
    existente = session.query(Organo).filter(Organo.codigo == codigo).one_or_none()
    if existente:
        _codigo_uuid[codigo] = existente.id
        logger.debug(f"Organo ya existente: codigo='{codigo}', nombre='{organo_dict.get('nombre')}'")
        return existente.id

    nuevo = Organo(codigo=codigo, id_padre=id_padre, **organo_dict)
    session.add(nuevo)
    session.flush()
    _codigo_uuid[codigo] = nuevo.id

    logger.info(
        f"Organo insertado: codigo='{codigo}', nombre='{nuevo.nombre}', tipo='{nuevo.tipo}'"
    )
    return nuevo.id


def insertar_geografico(session, id_num, padre_codigo, nombre):
    codigo = formar_codigo(TipoOrganoEnum.GEOGRAFICO, id_num)
    insertar_organo(session, {
        'codigo': codigo,
        'padre_codigo': padre_codigo,
        'nombre': nombre,
        'tipo': TipoOrganoEnum.GEOGRAFICO,
    })


def procesar_autonomicas(session, json_path=None):
    logger.info("Procesando organos autonomicos...")
    if json_path:
        logger.info(f"Cargando desde {json_path}")
        comunidades = _load_json(json_path)
    else:
        resp = requests.get(BASE_URL + "A", timeout=30)
        resp.raise_for_status()
        comunidades = resp.json()
    raiz_codigo = formar_codigo(TipoOrganoEnum.GEOGRAFICO, 0)

    for comunidad in comunidades:
        comunidad_codigo = formar_codigo(TipoOrganoEnum.GEOGRAFICO, comunidad['id'])
        comunidad_nombre = comunidad['descripcion']
        insertar_geografico(session, comunidad['id'], raiz_codigo, comunidad_nombre)

        for hijo in comunidad.get("children", []):
            hijo_codigo = formar_codigo(TipoOrganoEnum.AUTONOMICO, hijo['id'])
            hijo_nombre = hijo['descripcion']
            insertar_organo(session, {
                'codigo': hijo_codigo,
                'padre_codigo': comunidad_codigo,
                'nombre': hijo_nombre,
                'tipo': TipoOrganoEnum.AUTONOMICO,
                'nivel1': comunidad_nombre,
                'nivel2': hijo_nombre,
                'nivel3': None,
                'nivel1_norm': normalizar(comunidad_nombre),
                'nivel2_norm': normalizar(hijo_nombre),
                'nivel3_norm': None,
            })
    logger.info("Terminado el proceso de organos autonomicos.")


def procesar_estado(session, json_path=None):
    logger.info("Procesando organos estatales...")
    if json_path:
        logger.info(f"Cargando desde {json_path}")
        ministerios = _load_json(json_path)
    else:
        resp = requests.get(BASE_URL + "C", timeout=30)
        resp.raise_for_status()
        ministerios = resp.json()
    raiz_codigo = formar_codigo(TipoOrganoEnum.GEOGRAFICO, 0)

    for ministerio in ministerios:
        min_codigo = formar_codigo(TipoOrganoEnum.CENTRAL, ministerio['id'])
        min_nombre = ministerio['descripcion']
        insertar_organo(session, {
            'codigo': min_codigo,
            'padre_codigo': raiz_codigo,
            'nombre': min_nombre,
            'tipo': TipoOrganoEnum.CENTRAL,
            'nivel1': 'ESTADO',
            'nivel2': min_nombre,
            'nivel3': None,
            'nivel1_norm': 'ESTADO',
            'nivel2_norm': normalizar(min_nombre),
            'nivel3_norm': None,
        })
        for hijo in ministerio.get("children", []):
            hijo_codigo = formar_codigo(TipoOrganoEnum.CENTRAL, hijo['id'])
            hijo_nombre = hijo['descripcion']
            insertar_organo(session, {
                'codigo': hijo_codigo,
                'padre_codigo': min_codigo,
                'nombre': hijo_nombre,
                'tipo': TipoOrganoEnum.CENTRAL,
                'nivel1': 'ESTADO',
                'nivel2': min_nombre,
                'nivel3': hijo_nombre,
                'nivel1_norm': 'ESTADO',
                'nivel2_norm': normalizar(min_nombre),
                'nivel3_norm': normalizar(hijo_nombre),
            })


def procesar_otros(session, json_path=None):
    logger.info("Procesando otros organos...")
    if json_path:
        logger.info(f"Cargando desde {json_path}")
        otros = _load_json(json_path)
    else:
        resp = requests.get(BASE_URL + "O", timeout=30)
        resp.raise_for_status()
        otros = resp.json()
    raiz_codigo = formar_codigo(TipoOrganoEnum.GEOGRAFICO, 0)

    for otro in otros:
        otro_codigo = formar_codigo(TipoOrganoEnum.OTRO, otro['id'])
        otro_nombre = otro['descripcion']
        insertar_organo(session, {
            'codigo': otro_codigo,
            'padre_codigo': raiz_codigo,
            'nombre': otro_nombre,
            'tipo': TipoOrganoEnum.OTRO,
            'nivel1': 'OTROS',
            'nivel2': otro_nombre,
            'nivel3': None,
            'nivel1_norm': 'OTROS',
            'nivel2_norm': normalizar(otro_nombre),
            'nivel3_norm': None,
        })
        for hijo in otro.get("children", []):
            hijo_codigo = formar_codigo(TipoOrganoEnum.OTRO, hijo['id'])
            hijo_nombre = hijo['descripcion']
            insertar_organo(session, {
                'codigo': hijo_codigo,
                'padre_codigo': otro_codigo,
                'nombre': hijo_nombre,
                'tipo': TipoOrganoEnum.OTRO,
                'nivel1': 'OTROS',
                'nivel2': otro_nombre,
                'nivel3': hijo_nombre,
                'nivel1_norm': 'OTROS',
                'nivel2_norm': normalizar(otro_nombre),
                'nivel3_norm': normalizar(hijo_nombre),
            })


def procesar_locales(session, json_path=None):
    logger.info("Procesando organos locales (provincias, municipios, ayuntamientos)...")
    try:
        if json_path:
            logger.info(f"Cargando desde {json_path}")
            provincias = _load_json(json_path)
        else:
            resp = requests.get(BASE_URL + "L", timeout=30)
            resp.raise_for_status()
            provincias = resp.json()
        logger.info(f"Se han recibido {len(provincias)} provincias.")
    except Exception as e:
        logger.error(f"Error al obtener provincias locales: {e}")
        provincias = []

    for provincia in provincias:
        nombre_prov = provincia['descripcion']
        nombre_prov_norm = normalizar(nombre_prov)
        prov_codigo = formar_codigo(TipoOrganoEnum.GEOGRAFICO, provincia['id'])

        # Buscar la comunidad autonoma (padre) para la provincia
        padre_codigo = None
        ccaa_nombre = None
        for ccaa, lista_provs in MAPA_PROVINCIAS.items():
            lista_norm = [normalizar(p) for p in lista_provs]
            if nombre_prov_norm in lista_norm:
                ccaa_nombre = ccaa
                ccaa_obj = session.query(Organo).filter(
                    Organo.tipo == TipoOrganoEnum.GEOGRAFICO
                ).all()
                for posible in ccaa_obj:
                    if normalizar(posible.nombre) == normalizar(ccaa):
                        padre_codigo = posible.codigo
                        break
                break

        insertar_organo(session, {
            'codigo': prov_codigo,
            'padre_codigo': padre_codigo,
            'nombre': nombre_prov,
            'tipo': TipoOrganoEnum.GEOGRAFICO,
            'nivel1': ccaa_nombre,
            'nivel2': nombre_prov,
            'nivel3': None,
            'nivel1_norm': normalizar(ccaa_nombre) if ccaa_nombre else None,
            'nivel2_norm': nombre_prov_norm,
            'nivel3_norm': None,
        })

        for municipio in provincia.get('children', []):
            nombre_muni = municipio['descripcion']
            muni_codigo = formar_codigo(TipoOrganoEnum.GEOGRAFICO, municipio['id'])
            insertar_organo(session, {
                'codigo': muni_codigo,
                'padre_codigo': prov_codigo,
                'nombre': nombre_muni,
                'tipo': TipoOrganoEnum.GEOGRAFICO,
                'nivel1': ccaa_nombre,
                'nivel2': nombre_prov,
                'nivel3': nombre_muni,
                'nivel1_norm': normalizar(ccaa_nombre) if ccaa_nombre else None,
                'nivel2_norm': nombre_prov_norm,
                'nivel3_norm': normalizar(nombre_muni),
            })

            for ayto in municipio.get('children', []):
                nombre_ayto = ayto['descripcion']
                ayto_codigo = formar_codigo(TipoOrganoEnum.LOCAL, ayto['id'])
                insertar_organo(session, {
                    'codigo': ayto_codigo,
                    'padre_codigo': muni_codigo,
                    'nombre': nombre_ayto,
                    'tipo': TipoOrganoEnum.LOCAL,
                    'nivel1': nombre_muni,
                    'nivel2': nombre_ayto,
                    'nivel3': None,
                    'nivel1_norm': normalizar(nombre_muni),
                    'nivel2_norm': normalizar(nombre_ayto),
                    'nivel3_norm': None,
                })


def load_organos(session, json_paths=None):
    """
    Carga organos desde la API o desde archivos JSON locales.

    Args:
        session: Sesión de SQLAlchemy
        json_paths: Dict opcional con rutas a JSON {'C': path, 'A': path, 'L': path, 'O': path}
    """
    logger.info("Inicio del poblamiento de organos...")
    _codigo_uuid.clear()
    raiz_codigo = formar_codigo(TipoOrganoEnum.GEOGRAFICO, 0)

    if not session.query(Organo).filter(Organo.codigo == raiz_codigo).first():
        try:
            insertar_organo(session, {
                'codigo': raiz_codigo,
                'padre_codigo': None,
                'nombre': "ESTADO",
                'tipo': TipoOrganoEnum.GEOGRAFICO,
            })
            session.commit()
            logger.info("Nodo raiz geografico 'G0' insertado correctamente.")
        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Error al insertar el nodo raiz geografico 'G0': {e}")

    json_paths = json_paths or {}

    try:
        procesar_estado(session, json_paths.get('C'))
        session.commit()
        procesar_autonomicas(session, json_paths.get('A'))
        session.commit()
        procesar_locales(session, json_paths.get('L'))
        session.commit()
        procesar_otros(session, json_paths.get('O'))
        session.commit()
        logger.info("Poblamiento completado y cambios guardados.")
    except SQLAlchemyError as e:
        session.rollback()
        logger.error(f"Error al guardar cambios en la BD: {e}")
