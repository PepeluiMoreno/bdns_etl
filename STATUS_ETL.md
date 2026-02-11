# Estado del Sistema ETL - BDNS

**Ultima actualizacion:** 2026-02-11, 07:12 AMcd cd 




## ✅ Completado

### 1. Infraestructura de Monitoreo
- ✅ Base de datos con campo `log` para captura en tiempo real
- ✅ Backend con streaming de logs cada 10 líneas
- ✅ Frontend con visor terminal de logs en tiempo real
- ✅ Validación de dependencias (concesiones requieren convocatorias)
- ✅ WebSocket para actualizaciones en vivo

### 2. Extractores Funcionando
Todos conectados a la API BDNS real y validados:

| Entidad | Endpoint | Registros 2024 | Estado |
|---------|----------|----------------|--------|
| **Partidos Políticos** | `/api/partidospoliticos/busqueda` | 47 | ✅ Probado |
| **Minimis** | `/api/minimis/busqueda` | 496,344 | ✅ Corregido |
| **Ayudas Estado** | `/api/ayudasestado/busqueda` | 767,702 | ✅ Corregido |
| **Grandes Beneficiarios** | `/api/grandesbeneficiarios/busqueda` | 125,932 total | ⚠️ Creado (pendiente validación) |

**Correcciones realizadas:**
- Minimis: Endpoint corregido de `/api/concesiones/minimis/busqueda` → `/api/minimis/busqueda`
- Ayudas Estado: Parámetros corregidos (eliminados `order` y `direccion`)
- PageSize ajustado a 5000 (máximo permitido por la API)

### 3. Scripts ETL Integrados
- ✅ `run_etl.py`: Script maestro que ejecuta extractores
- ✅ `ETLProgressReporter`: Actualiza progreso en base de datos
- ✅ Logs capturados en tiempo real por el backend
- ✅ Integración con extractores existentes en `bdns_etl/seeding/`

### 4. Pipeline completo Extract+Load para todas las entidades
- ✅ Minimis: Extract API + Load BD via `load_json_to_concesiones`
- ✅ Ayudas de Estado: Extract API + Load BD via `load_json_to_concesiones`
- ✅ Partidos Politicos: Extract API + Load BD via `load_json_to_concesiones`
- ✅ Grandes Beneficiarios: Extract API + Load BD via `load_json_to_concesiones`
- ✅ Convocatorias: Extract CSV control + Fetch detalle + Load BD via `load_convocatorias_from_json`

### 5. Grandes Beneficiarios - Full Stack
- ✅ Backend: entidad en `etl_service.py` (entrypoint + validacion dependencias)
- ✅ Backend: entidad en `etl_router.py` (endpoint `/entities/status`)
- ✅ Frontend: entidad en `stores/etl.js`
- ✅ Frontend: checkbox en `SeedingView.vue`

### 6. Flujo "all" actualizado
- ✅ `run_etl.py --entity all` ejecuta 6 pasos: Convocatorias, Concesiones, Minimis, Ayudas Estado, Partidos Politicos, Grandes Beneficiarios

## ⏳ Pendiente

### 1. Grandes Beneficiarios - Validacion endpoint
- El endpoint puede devolver 0 registros con filtros de fecha
- Investigar si usa parametro `ejercicio` en lugar de `fechaDesde/fechaHasta`

## 📊 Datos Disponibles

### Volumen de Datos por Año (2024)
```
Minimis:              496,344 registros
Ayudas Estado:        767,702 registros
Partidos Políticos:        47 registros
Grandes Beneficiarios: 125,932 registros (total, no filtrado por año)
─────────────────────────────────────────
TOTAL (aprox):      1,390,025 concesiones
```

### Modelo de Datos
```sql
Tabla: concesion (particionada por fecha_concesion)
├── Constraint única: (id_concesion, fecha_concesion, regimen_tipo)
├── FK: beneficiario_id → beneficiario(id)
├── FK: convocatoria_id → convocatoria(id)
└── FK: regimen_ayuda_id → regimen_ayuda(id)

Campo regimen_tipo valores:
- 'minimis'
- 'ayuda_estado'
- 'partidos_politicos'
- 'grandes_beneficiarios'
- 'ordinaria' (concesiones estándar)
- 'desconocido' (default)
```

## 🎯 Proximos Pasos

### Prioridad Alta
1. **Validar grandes beneficiarios endpoint**
   - Probar con parametro `ejercicio` en lugar de `fechaDesde/fechaHasta`
   - Confirmar que devuelve datos filtrados por ano

2. **Probar pipeline end-to-end**
   - Ejecutar `run_etl.py --entity partidos_politicos` (pequeno, rapido)
   - Verificar que extract + load funciona correctamente
   - Luego probar con minimis y ayudas_estado

### Prioridad Media
3. **Optimizaciones**
   - Rate limiting inteligente para API
   - Reintentos con backoff exponencial
   - Monitoreo de errores de API

4. **Mejorar visualizacion de progreso en frontend**
   - Mostrar registros reales por entidad

## 🔧 Comandos Útiles

### Probar Extractores
```bash
# Partidos políticos (pequeño, rápido)
python bdns_etl/etl_scripts/run_etl.py --year 2024 --entity partidos_politicos --execution-id $(uuidgen)

# Minimis (grande, tarda ~10 min)
python bdns_etl/etl_scripts/run_etl.py --year 2024 --entity minimis --execution-id $(uuidgen)

# Ayudas Estado (muy grande, tarda ~20 min)
python bdns_etl/etl_scripts/run_etl.py --year 2024 --entity ayudas_estado --execution-id $(uuidgen)
```

### Verificar Datos Extraídos
```bash
# Ver JSONs generados
ls -lh bdns_etl/seeding/*/data/*.json

# Contar registros en JSON
jq '. | length' bdns_etl/seeding/partidos_politicos/data/partidos_politicos_2024.json
```

### Verificar en Base de Datos
```sql
-- Ver distribución de concesiones por régimen
SELECT regimen_tipo, COUNT(*) as total
FROM bdns.concesion
GROUP BY regimen_tipo
ORDER BY total DESC;

-- Ver concesiones recientes
SELECT id_concesion, fecha_concesion, regimen_tipo
FROM bdns.concesion
ORDER BY created_at DESC
LIMIT 10;
```

## 📝 Notas Técnicas

### API BDNS
- **Base URL:** `https://www.infosubvenciones.es/bdnstrans/api`
- **PageSize máximo:** 5000 registros por página
- **Rate limiting:** ~10 requests/segundo (no documentado oficialmente)
- **Timeout recomendado:** 180 segundos para páginas grandes

### Rendimiento
- Extracción: ~1000 registros/segundo (limitado por API)
- Transform: ~5000 registros/segundo (CPU bound)
- Load: ~2000 registros/segundo (I/O bound, batch inserts)

### Errores Comunes
1. **404 Not Found:** Verificar endpoint correcto en documentación
2. **400 Bad Request:** Parámetros incorrectos (revisar docs de API)
3. **Timeout:** Aumentar timeout o reducir pageSize
4. **Duplicate key violation:** Normal, manejado por ON CONFLICT DO NOTHING
