# Nuevas Entidades ETL - Retención Extendida

## Contexto

Se han incorporado tres **nuevos extractores** para endpoints específicos de la API BDNS que contienen datos con **requisitos especiales de retención** (10 años según normativa europea) y que pueden no aparecer en los endpoints regulares de concesiones.

## Entidades Añadidas

### 1. 🟣 Ayudas de Minimis

**Archivo:** `bdns_etl/seeding/minimis/extract_minimis.py`

**Endpoint API:** `/api/concesiones/minimis/busqueda` (puede variar)

**Características:**
- Retención obligatoria de **10 años** según normativa UE
- Contiene ayudas de minimis que pueden no aparecer en endpoints regulares
- Requisitos especiales de publicación y transparencia

**Uso:**
```bash
python extract_minimis.py --year 2024 --output-dir ./data
```

### 2. 🟣 Ayudas de Estado

**Archivo:** `bdns_etl/seeding/ayudas_estado/extract_ayudas_estado.py`

**Endpoint API:** `/api/ayudasestado/busqueda`

**Características:**
- Retención obligatoria de **10 años** según normativa UE
- Ayudas de estado sujetas a control de la Comisión Europea
- Requisitos especiales de notificación

**Uso:**
```bash
python extract_ayudas_estado.py --year 2024 --output-dir ./data
```

### 3. 🟣 Partidos Políticos

**Archivo:** `bdns_etl/seeding/partidos_politicos/extract_partidos_politicos.py`

**Endpoint API:** `/api/partidospoliticos/busqueda`

**Características:**
- Contiene concesiones y beneficiarios específicos de partidos políticos
- **Datos que NO aparecen** en el endpoint regular de concesiones
- Requisitos especiales de transparencia y publicación

**Uso:**
```bash
python extract_partidos_politicos.py --year 2022 --output-dir ./data
```

**Ejemplo de curl verificado:**
```bash
curl -X 'GET' \
  'https://www.infosubvenciones.es/bdnstrans/api/partidospoliticos/busqueda?page=0&pageSize=50&order=numeroConvocatoria&direccion=asc&vpd=GE&descripcion=Resoluci%C3%B3n&descripcionTipoBusqueda=0&numeroConvocatoria=%20&codConcesion=%20&fechaDesde=01%2F01%2F2022&fechaHasta=31%2F12%2F2022&nifCif=%20' \
  -H 'accept: application/json'
```

## Integración en el Sistema

### Backend

Las nuevas entidades están integradas en el servicio ETL:

```python
# bdns_etl/backend/src/bdns_etl/services/etl_service.py

entrypoint_map = {
    "convocatorias": "ETL/convocatorias/orchestrator_convocatorias.py",
    "concesiones": "ETL/concesiones/orchestrator_concesiones.py",
    "catalogos": "ETL/seeding/catalogos/load/load_all_catalogos.py",
    "minimis": "ETL/seeding/minimis/extract_minimis.py",              # NUEVO
    "ayudas_estado": "ETL/seeding/ayudas_estado/extract_ayudas_estado.py",  # NUEVO
    "partidos_politicos": "ETL/seeding/partidos_politicos/extract_partidos_politicos.py"  # NUEVO
}
```

### Frontend

En la vista de Seeding ([SeedingView.vue](../frontend/src/views/SeedingView.vue)), las nuevas entidades aparecen en una sección separada:

```
PRINCIPALES
├── Convocatorias
├── Concesiones
└── Catálogos

RETENCIÓN EXTENDIDA (10 AÑOS)
├── Ayudas de Minimis (10 años)
├── Ayudas de Estado (10 años)
└── Partidos Políticos (especial)
```

## Pruebas

Se incluye un script de prueba para verificar la conectividad:

```bash
cd /home/jose/dev/bdns/bdns_etl/seeding
python test_new_extractors.py
```

Este script:
1. ✅ Verifica el endpoint de Partidos Políticos (confirmado funcional)
2. ⚠️ Prueba los endpoints de Minimis y Ayudas de Estado (pueden requerir ajustes)

## Próximos Pasos

### 1. Verificación de Endpoints

Ejecutar pruebas para confirmar los endpoints exactos:

```bash
python test_new_extractors.py
```

Si los endpoints de Minimis o Ayudas de Estado fallan, consultar:
- [Documentación oficial BDNS](https://www.pap.hacienda.gob.es/bdnstrans)
- [FAQ BDNS 2025](https://www.oficinavirtual.pap.hacienda.gob.es/sitios/oficinavirtual/es-ES/CatalogoSistemasInformacion/TESEOnet/Documents/RESPUESTAS%20FRECUENTES%20BDNS%20FAQ%202025.pdf)

### 2. Ajuste de Parámetros

Los extractores pueden requerir ajustes en:
- Nombres de parámetros
- Estructura de respuesta JSON
- Headers adicionales
- Autenticación (si aplica)

### 3. Transformación y Carga

Crear los scripts de transformación y carga para:
- Normalizar datos según esquema de BD
- Manejar duplicados y actualizaciones
- Integrar con `ETLProgressReporter` para progreso en tiempo real

### 4. Integración con Orchestrator

Actualizar `run_etl.py` para manejar las nuevas entidades en modo batch.

## Normativa

### Retención de 10 Años

Según el [Real Decreto 130/2019](https://www.boe.es/buscar/act.php?id=BOE-A-2019-4671):

> La información relativa a ayudas de Estado y ayudas de minimis se mantendrá en el SNPSAP durante **10 años** desde la fecha de concesión.

Esto justifica la necesidad de endpoints específicos que garanticen la disponibilidad completa de estos datos.

## Referencias

- 📚 [Sistema Nacional de Publicidad de Subvenciones](https://www.pap.hacienda.gob.es/bdnstrans/GE/es/inicio)
- 📚 [Portal InfoSubvenciones](https://www.subvenciones.gob.es/)
- 📚 [Partidos Políticos - BDNS](https://www.pap.hacienda.gob.es/bdnstrans/GE/es/partidospoliticos)
- 📚 [Base de Datos Nacional de Subvenciones - datos.gob.es](https://datos.gob.es/en/catalogo/e05188501-base-de-datos-nacional-de-subvenciones)
- 📚 [Real Decreto 130/2019 (BOE)](https://www.boe.es/buscar/act.php?id=BOE-A-2019-4671)

## Contacto

Para dudas o ajustes en los extractores, verificar primero:
1. Documentación oficial de la API BDNS
2. Logs de ejecución del ETL
3. Respuestas de la API (status codes, mensajes de error)

---

**Fecha de creación:** 2026-02-09
**Autor:** Sistema ETL BDNS
**Versión:** 1.0
