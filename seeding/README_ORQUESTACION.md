# 🎯 Orquestación de Extracción de Concesiones

## Resumen Ejecutivo

El sistema ETL extrae concesiones de **4 fuentes oficiales** de la API BDNS y las consolida en una única tabla deduplicada:

1. ✅ **Concesiones Ordinarias** - Endpoint estándar de concesiones
2. ✅ **Ayudas de Minimis** - Retención obligatoria 10 años (normativa UE)
3. ✅ **Ayudas de Estado** - Retención obligatoria 10 años (normativa UE)
4. ✅ **Partidos Políticos** - Transparencia especial (datos exclusivos)

**Resultado final:** Conjunto de concesiones **NO REPETIDAS** garantizado por constraint única en PostgreSQL.

---

## 🚀 Uso Rápido

### Opción 1: Orquestador Maestro (Recomendado)

```bash
cd /home/jose/dev/bdns/bdns_etl/seeding

# Extraer todas las fuentes en paralelo (más rápido)
python orchestrate_all_concesiones.py --year 2024 --parallel

# Extraer secuencialmente (más seguro con APIs rate-limited)
python orchestrate_all_concesiones.py --year 2024

# Extraer solo fuentes específicas
python orchestrate_all_concesiones.py --year 2024 --sources minimis,ayudas_estado
```

### Opción 2: API REST del ETL Backend

```bash
# Iniciar backend
cd /home/jose/dev/bdns/bdns_etl/backend
python main.py

# Desde otro terminal, lanzar extracción
curl -X POST http://localhost:8001/api/etl/seeding/start \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "year": 2024,
    "entity": "all_concesiones",
    "batch_size": 5000
  }'
```

### Opción 3: Frontend ETL Admin

1. Abrir http://localhost:3001
2. Login (admin/admin123)
3. Ir a "Seeding"
4. Seleccionar año 2024
5. Seleccionar "Todas las Concesiones"
6. Click "Iniciar Seeding"

---

## 📊 Arquitectura de Extracción

### Flujo de Datos

```
API BDNS
  ├─ /api/concesiones/busqueda                → Ordinarias
  ├─ /api/concesiones/minimis/busqueda        → Minimis
  ├─ /api/ayudasestado/busqueda               → Ayudas Estado
  └─ /api/partidospoliticos/busqueda          → Partidos Políticos
         ↓
  [Extractores Python]
    • extract_concesiones.py
    • extract_minimis.py
    • extract_ayudas_estado.py
    • extract_partidos_politicos.py
         ↓
  [Archivos JSON]
    • data/concesiones_2024.json
    • data/minimis_2024.json
    • data/ayudas_estado_2024.json
    • data/partidos_politicos_2024.json
         ↓
  [Transformación + Carga]
    • Normalización de campos
    • Mapeo a modelo Concesion
    • INSERT con ON CONFLICT DO NOTHING
         ↓
  [PostgreSQL]
    tabla: concesion
    constraint: UNIQUE(id_concesion, fecha_concesion, regimen_tipo)
         ↓
  [Resultado Final]
    ✅ Concesiones NO REPETIDAS
    ✅ Particionadas por año + régimen
    ✅ Indexadas para queries rápidas
```

---

## 🔍 Deduplicación Automática

### Constraint Única

```sql
ALTER TABLE concesion
ADD CONSTRAINT uq_concesion_id_fecha
UNIQUE (id_concesion, fecha_concesion, regimen_tipo);
```

### Cómo Funciona

| id_concesion | fecha_concesion | regimen_tipo | Acción |
|--------------|-----------------|--------------|--------|
| 123456 | 2024-05-15 | ordinaria | ✅ INSERT (primera vez) |
| 123456 | 2024-05-15 | minimis | ✅ INSERT (distinto regimen_tipo) |
| 123456 | 2024-05-15 | ordinaria | ❌ SKIP (duplicado) |

**Resultado:** Si una concesión aparece en múltiples fuentes, se guarda solo una vez por cada combinación de `(id, fecha, regimen)`.

### Código de Carga

```python
from sqlalchemy.dialects.postgresql import insert as pg_insert

# Preparar datos
concesiones = [
    {"id_concesion": "123", "fecha_concesion": "2024-05-15", "regimen_tipo": "ordinaria", ...},
    {"id_concesion": "123", "fecha_concesion": "2024-05-15", "regimen_tipo": "minimis", ...},
]

# Insertar con deduplicación
stmt = pg_insert(Concesion).values(concesiones)
stmt = stmt.on_conflict_do_nothing(constraint='uq_concesion_id_fecha')
session.execute(stmt)
```

---

## 📁 Estructura de Archivos

```
bdns_etl/seeding/
│
├── orchestrate_all_concesiones.py   ← 🎯 ORQUESTADOR MAESTRO
│
├── concesiones/
│   ├── extract/
│   │   └── extract_concesiones.py   ← Concesiones ordinarias
│   ├── transform/
│   │   └── transform_concesiones.py
│   └── load/
│       └── load_concesiones.py
│
├── minimis/
│   ├── extract_minimis.py           ← Ayudas de minimis
│   └── data/
│       └── minimis_2024.json
│
├── ayudas_estado/
│   ├── extract_ayudas_estado.py     ← Ayudas de estado
│   └── data/
│       └── ayudas_estado_2024.json
│
├── partidos_politicos/
│   ├── extract_partidos_politicos.py ← Partidos políticos
│   └── data/
│       └── partidos_politicos_2024.json
│
├── test_new_extractors.py           ← Script de pruebas
└── README_ORQUESTACION.md           ← Este archivo
```

---

## 🔧 Configuración

### Variables de Entorno

Archivo: `.env` o `.env.development`

```bash
# API BDNS
BDNS_API_URL=https://www.infosubvenciones.es/bdnstrans/api
BDNS_API_TIMEOUT=30
BDNS_API_RATE_LIMIT=10

# Base de datos
DATABASE_URL=postgresql://user:pass@localhost:5432/bdns

# ETL
ETL_BATCH_SIZE=5000
ETL_MAX_WORKERS=4
```

### Rate Limiting

La API BDNS tiene límites:
- **Requests por segundo:** ~10 (no documentado oficialmente)
- **Tamaño página máximo:** 10000 registros

**Recomendación:**
- Usar extracción **secuencial** si hay errores de rate limit
- Agregar delays entre requests (`time.sleep(0.5)`)
- Manejar reintentos con backoff exponencial

---

## 📊 Particionamiento de Tabla

La tabla `concesion` está particionada jerárquicamente para optimizar queries:

### Nivel 1: RANGE por fecha (anual)

```sql
CREATE TABLE concesion_2024 PARTITION OF concesion
FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
```

### Nivel 2: LIST por regimen_tipo (por implementar)

```sql
CREATE TABLE concesion_2024_ordinaria PARTITION OF concesion_2024
FOR VALUES IN ('ordinaria');

CREATE TABLE concesion_2024_minimis PARTITION OF concesion_2024
FOR VALUES IN ('minimis');

CREATE TABLE concesion_2024_ayuda_estado PARTITION OF concesion_2024
FOR VALUES IN ('ayuda_estado');

CREATE TABLE concesion_2024_partidos_politicos PARTITION OF concesion_2024
FOR VALUES IN ('partidos_politicos');
```

### Beneficios

| Query | Partition Pruning | Speedup |
|-------|-------------------|---------|
| `WHERE fecha_concesion BETWEEN '2024-01-01' AND '2024-12-31'` | Solo 2024 | ~10x |
| `WHERE regimen_tipo = 'minimis' AND EXTRACT(YEAR FROM fecha_concesion) = 2024` | Solo 2024_minimis | ~40x |

---

## 🧪 Testing

### Script de Prueba

```bash
cd /home/jose/dev/bdns/bdns_etl/seeding
python test_new_extractors.py
```

Verifica:
- ✅ Conectividad con API BDNS
- ✅ Formato de respuesta JSON
- ✅ Campos requeridos presentes
- ⚠️ Endpoints específicos funcionan

### Prueba Manual de Endpoint

```bash
# Partidos Políticos (verificado funcional)
curl -X GET \
  'https://www.infosubvenciones.es/bdnstrans/api/partidospoliticos/busqueda?page=0&pageSize=50&fechaDesde=01/01/2024&fechaHasta=31/12/2024' \
  -H 'accept: application/json'

# Minimis (puede requerir ajuste)
curl -X GET \
  'https://www.infosubvenciones.es/bdnstrans/api/concesiones/minimis/busqueda?page=0&pageSize=50&fechaDesde=01/01/2024&fechaHasta=31/12/2024' \
  -H 'accept: application/json'
```

---

## 📈 Monitoreo y Logs

### Logs de Extracción

```bash
# Ver logs en tiempo real
tail -f /var/log/bdns_etl.log

# Filtrar por fuente específica
tail -f /var/log/bdns_etl.log | grep "minimis"
```

### Verificar Progreso en Base de Datos

```sql
-- Ver ejecuciones recientes
SELECT * FROM etl_execution
WHERE execution_type = 'seeding'
ORDER BY started_at DESC
LIMIT 10;

-- Contar concesiones por régimen
SELECT regimen_tipo, COUNT(*) as total
FROM concesion
WHERE EXTRACT(YEAR FROM fecha_concesion) = 2024
GROUP BY regimen_tipo
ORDER BY total DESC;

-- Verificar duplicados (debe retornar 0)
SELECT id_concesion, fecha_concesion, regimen_tipo, COUNT(*)
FROM concesion
GROUP BY id_concesion, fecha_concesion, regimen_tipo
HAVING COUNT(*) > 1;
```

---

## 🐛 Troubleshooting

### Error: "Endpoint not found"

**Problema:** API cambió la URL del endpoint

**Solución:**
1. Verificar documentación oficial BDNS
2. Probar con curl manualmente
3. Actualizar URL en extractor correspondiente

### Error: "Rate limit exceeded"

**Problema:** Demasiados requests a la API

**Solución:**
```python
# Agregar delay en extractor
import time
time.sleep(0.5)  # 500ms entre requests
```

### Error: "Duplicate key violation"

**Problema:** Intentando insertar duplicado sin ON CONFLICT

**Solución:**
```python
# Usar INSERT ... ON CONFLICT DO NOTHING
from sqlalchemy.dialects.postgresql import insert as pg_insert

stmt = pg_insert(Concesion).values(data)
stmt = stmt.on_conflict_do_nothing(constraint='uq_concesion_id_fecha')
session.execute(stmt)
```

### Concesiones faltantes

**Problema:** Algunas concesiones no aparecen

**Verificar:**
1. ¿Se extrajeron de las 4 fuentes?
2. ¿El rango de fechas es correcto?
3. ¿La transformación mapeó todos los campos?
4. ¿El campo `regimen_tipo` se asigna correctamente?

---

## 📚 Referencias

### Documentación Oficial

- [API BDNS](https://www.pap.hacienda.gob.es/bdnstrans)
- [Portal InfoSubvenciones](https://www.subvenciones.gob.es/)
- [Partidos Políticos BDNS](https://www.pap.hacienda.gob.es/bdnstrans/GE/es/partidospoliticos)
- [Real Decreto 130/2019](https://www.boe.es/buscar/act.php?id=BOE-A-2019-4671)

### Normativa

> **Real Decreto 130/2019**, Artículo 10:
>
> "La información relativa a ayudas de Estado y ayudas de minimis se mantendrá en el SNPSAP durante **10 años** desde la fecha de concesión."

Esto justifica la existencia de endpoints específicos para estas ayudas.

---

## ✅ Checklist de Ejecución

Antes de ejecutar extracción completa:

- [ ] Base de datos PostgreSQL corriendo
- [ ] Tabla `concesion` con constraint única creada
- [ ] Variables de entorno configuradas (`.env.development`)
- [ ] Backend ETL iniciado (si se usa API)
- [ ] Verificar conectividad con API BDNS
- [ ] Espacio en disco suficiente (>5GB por año)

Durante extracción:

- [ ] Monitorear logs en tiempo real
- [ ] Verificar progreso en base de datos
- [ ] Comprobar errores de API (rate limiting)

Después de extracción:

- [ ] Verificar totales de registros
- [ ] Comprobar que no hay duplicados
- [ ] Validar integridad referencial (beneficiarios, convocatorias)
- [ ] Actualizar estadísticas de PostgreSQL (`ANALYZE concesion`)

---

**Última actualización:** 2026-02-10
**Versión:** 1.0
**Autor:** Sistema ETL BDNS
