# BDNS ETL

Sistema ETL (Extract-Transform-Load) para la Base de Datos Nacional de Subvenciones. Extrae datos de la API oficial de BDNS, los transforma y carga en PostgreSQL. Incluye un backend FastAPI con API REST y un frontend Vue 3 para administracion.

## Que hace

1. **Extrae** datos de subvenciones desde 5 endpoints de la API oficial de BDNS
2. **Transforma** y normaliza (deduplicacion, validacion, enriquecimiento)
3. **Carga** en PostgreSQL con particionamiento por ano y tipo de regimen
4. **Sincroniza** incrementalmente (sync semanal)
5. **Monitoriza** en tiempo real via WebSocket

## Fuentes de datos

| Fuente | Endpoint API | Volumen aprox. (2024) |
|---|---|---|
| Concesiones ordinarias | `/api/concesiones/busqueda` | 1.4M+ registros |
| Ayudas de minimis | `/api/minimis/busqueda` | ~496K registros |
| Ayudas de estado | `/api/ayudasestado/busqueda` | ~768K registros |
| Partidos politicos | `/api/partidospoliticos/busqueda` | ~50 registros |
| Grandes beneficiarios | `/api/grandesbeneficiarios/busqueda` | ~126K registros |

## Stack

| Componente | Tecnologia |
|---|---|
| Backend API | FastAPI + Uvicorn (puerto 8001) |
| Frontend admin | Vue 3 + Vite + TailwindCSS (puerto 3001) |
| State management | Pinia |
| Base de datos | PostgreSQL 15 (particionado) |
| ORM | SQLAlchemy 2.0 + asyncpg |
| Graficas | Chart.js |
| Dependencia core | bdns_core |

## Estructura

```
bdns_etl/
├── backend/                        # API REST
│   ├── src/bdns_etl/
│   │   ├── api/
│   │   │   ├── auth.py             # Autenticacion JWT
│   │   │   └── etl_router.py       # Endpoints ETL
│   │   └── services/               # Orquestacion
│   └── main.py
│
├── frontend/                       # Panel de administracion
│   └── src/
│       ├── views/                  # Dashboard, Seeding, Sync, Ejecuciones
│       ├── stores/                 # auth, etl, system
│       └── components/
│
├── seeding/                        # Pipelines de carga inicial
│   ├── concesiones/                # Extract > Transform > Load
│   ├── minimis/
│   ├── ayudas_estado/
│   ├── partidos_politicos/
│   ├── grandes_beneficiarios/
│   ├── convocatorias/
│   ├── catalogos/
│   └── orchestrate_all_concesiones.py  # Orquestador maestro
│
├── sync/                           # Sincronizacion incremental
│   ├── sync_weekly.py
│   └── sync_catalogos.py
│
├── Dockerfile
└── Dockerfile.frontend
```

## Inicio rapido

```bash
# Backend
cd backend
pip install -e .
python main.py                      # http://localhost:8001

# Frontend
cd frontend
npm install
npm run dev                         # http://localhost:3001
```

## API REST

| Metodo | Endpoint | Auth | Descripcion |
|---|---|---|---|
| GET | `/api/etl/system-status` | No | Estado del sistema |
| GET | `/api/etl/executions` | JWT | Historial de ejecuciones |
| GET | `/api/etl/statistics` | JWT | Estadisticas ETL |
| GET | `/api/etl/sync-control` | JWT | Estado de sincronizacion |
| POST | `/api/etl/seeding/start` | Admin | Iniciar carga |
| POST | `/api/etl/sync/start` | Admin | Iniciar sincronizacion |
| WS | `/api/etl/ws` | JWT | Logs en tiempo real |

## ETL por linea de comandos

```bash
# Seeding completo de un ano (todas las fuentes en paralelo)
python seeding/orchestrate_all_concesiones.py --year 2024 --parallel

# Sync semanal incremental
python sync/sync_weekly.py

# Actualizar catalogos (tablas maestras)
python sync/sync_catalogos.py
```

## Deduplicacion

Los mismos datos pueden aparecer en multiples fuentes. Se almacenan una vez por combinacion unica:

```sql
UNIQUE (id_concesion, fecha_concesion, regimen_tipo)
-- ON CONFLICT DO NOTHING
```

## Particionamiento de datos

```
concesion (tabla particionada)
├── concesion_2024
│   ├── concesion_2024_ordinaria
│   ├── concesion_2024_minimis
│   ├── concesion_2024_ayudas_estado
│   └── concesion_2024_partidos_politicos
└── concesion_2023
    └── ...
```

## Credenciales por defecto

| Rol | Usuario | Password |
|---|---|---|
| Admin | admin | admin123 |
| User | user | user123 |

## Variables de entorno

```bash
BDNS_API_URL=https://www.infosubvenciones.es/bdnstrans/api
DATABASE_URL=postgresql://user:pass@localhost:5432/bdns
ETL_BATCH_SIZE=1000
ETL_MAX_WORKERS=4
CORS_ORIGINS=http://localhost:3001
```

## Licencia

MIT
