# Separación del Proyecto BDNS en Dos Aplicaciones

**Fecha:** 2026-02-08
**Autor:** Claude Code
**Objetivo:** Dividir el monolito BDNS en dos aplicaciones independientes con autenticación compartida

---

## 📋 Resumen Ejecutivo

El proyecto BDNS se ha dividido en **dos aplicaciones independientes**:

1. **BDNS Search** - Frontend y backend público para búsqueda de subvenciones
2. **ETL Admin** - Frontend y backend interno para administración de procesos ETL

Ambas aplicaciones comparten:
- Base de datos PostgreSQL (mismo esquema)
- Paquete `bdns_core` (modelos, utilidades, autenticación)
- Scripts ETL (`apps/ETL/`)
- Sistema de autenticación JWT

---

## 🏗️ Nueva Estructura del Proyecto

```
bdns/
├── apps/
│   ├── bdns-search-frontend/     # 🌐 Frontend público (Vue3) - Puerto 3000
│   │   ├── src/
│   │   │   ├── views/            # Búsqueda, detalles, estadísticas
│   │   │   └── apollo/           # Cliente GraphQL
│   │   └── package.json
│   │
│   ├── bdns-search-backend/      # 🔌 API GraphQL pública - Puerto 8000
│   │   ├── main.py
│   │   ├── alembic/              # Migraciones DB
│   │   └── src/bdns_api/
│   │       ├── graphql/          # Schema GraphQL, resolvers, types
│   │       └── services/         # Redis cache, notificaciones
│   │
│   ├── etl-admin-frontend/       # 🔧 Frontend admin (Vue3) - Puerto 3001
│   │   ├── src/
│   │   │   ├── views/            # Dashboard, Seeding, Sync, Executions
│   │   │   ├── stores/           # Pinia (auth, etl)
│   │   │   └── composables/      # useWebSocket, useETL
│   │   └── package.json
│   │
│   ├── etl-admin-backend/        # ⚙️ API REST admin - Puerto 8001
│   │   ├── main.py
│   │   └── src/etl_admin/
│   │       ├── api/              # Routers (auth, etl)
│   │       └── services/         # ETL service, job manager
│   │
│   └── ETL/                      # 📦 Scripts ETL compartidos
│       ├── concesiones/
│       ├── convocatorias/
│       └── run_etl.py
│
├── packages/
│   └── bdns_core/                # 🧩 Paquete Python compartido
│       └── src/bdns_core/
│           ├── db/               # Models, sessions, base
│           │   ├── models.py         # Modelos de negocio
│           │   ├── etl_models.py     # Modelos de control ETL
│           │   └── session.py
│           ├── auth/             # Sistema de autenticación JWT (NUEVO)
│           │   └── jwt_auth.py
│           └── business/         # Lógica de negocio compartida
│
├── data/                         # Datos de seed, CSVs, etc.
├── docs/                         # Documentación
└── docker-compose.yml            # Orquestación de servicios
```

---

## 🔐 Sistema de Autenticación Compartido

### Implementación

**Ubicación:** `packages/bdns_core/src/bdns_core/auth/jwt_auth.py`

**Características:**
- JWT tokens con firma HS256
- Access tokens (30 min) + Refresh tokens (7 días)
- Roles de usuario: `admin`, `user`
- Password hashing con bcrypt
- Funciones compartidas entre backends

### Configuración

**Variables de entorno (.env):**
```bash
# JWT Configuration
JWT_SECRET_KEY=tu-clave-secreta-cambiar-en-produccion
JWT_ALGORITHM=HS256
JWT_ACCESS_TOKEN_EXPIRE_MINUTES=30
JWT_REFRESH_TOKEN_EXPIRE_DAYS=7
```

**⚠️ IMPORTANTE:** En producción, usar una clave secreta segura generada con:
```bash
openssl rand -hex 32
```

### Flujo de Autenticación

```
┌─────────────┐      POST /api/auth/login       ┌──────────────────┐
│   Frontend  │ ──────────────────────────────▶ │ etl-admin-backend│
│  (Vue3)     │ { username, password }          │    (FastAPI)     │
└─────────────┘                                  └──────────────────┘
       │                                                  │
       │            { access_token, refresh_token }      │
       │ ◀────────────────────────────────────────────── │
       │                                                  │
       │      GET /api/etl/statistics                    │
       │      Authorization: Bearer <token>              │
       │ ──────────────────────────────────────────────▶ │
       │                                                  │
       │            { statistics: {...} }                │
       │ ◀────────────────────────────────────────────── │
```

### Usuarios de Prueba

**Admin:**
- Username: `admin`
- Password: `admin123`
- Role: `admin`
- Permisos: Lanzar procesos ETL, ver todo

**User:**
- Username: `user`
- Password: `user123`
- Role: `user`
- Permisos: Solo lectura (estadísticas, ejecuciones)

---

## 🌐 BDNS Search (Aplicación Pública)

### Frontend
- **Framework:** Vue3 + Vite
- **GraphQL Client:** graphql-request
- **Puerto:** 3000
- **URL:** http://localhost:3000

**Funcionalidades:**
- Búsqueda avanzada de convocatorias y concesiones
- Filtros por órgano, beneficiario, fecha, importe
- Visualización de estadísticas
- Mapas geográficos (regiones)
- Gráficos (Chart.js)

### Backend
- **Framework:** FastAPI + Strawberry GraphQL
- **Puerto:** 8000
- **URL:** http://localhost:8000/graphql

**Endpoints:**
- `GET /graphql` - GraphQL Playground
- `GET /health` - Health check

**Caché:**
- Redis con TTL de 1 hora
- Invalidación manual por entidad

---

## 🔧 ETL Admin (Aplicación Interna)

### Frontend
- **Framework:** Vue3 + Vite + Vue Router + Pinia
- **Estilos:** TailwindCSS
- **Puerto:** 3001
- **URL:** http://localhost:3001

**Rutas:**
```
/login          - Autenticación
/               - Dashboard (requiere auth)
/seeding        - Lanzar seeding (requiere admin)
/sync           - Lanzar sync (requiere admin)
/executions     - Historial de ejecuciones (requiere auth)
```

**Componentes Principales:**
- `LoginView.vue` - Formulario de login
- `DashboardView.vue` - Vista principal con estadísticas + WebSocket
- `SeedingView.vue` - Control de carga inicial (en desarrollo)
- `SyncView.vue` - Control de sincronización (en desarrollo)
- `ExecutionsView.vue` - Historial detallado (en desarrollo)

**Store (Pinia):**
- `auth.js` - Gestión de autenticación (login, logout, refresh)

**WebSocket:**
- Conexión a `ws://localhost:8001/api/etl/ws`
- Recibe actualizaciones cada 1 segundo
- Reconexión automática

### Backend
- **Framework:** FastAPI
- **Puerto:** 8001
- **URL:** http://localhost:8001/docs

**Endpoints REST:**

| Método | Endpoint | Auth | Descripción |
|--------|----------|------|-------------|
| POST | `/api/auth/login` | No | Login (retorna JWT) |
| POST | `/api/auth/refresh` | No | Renovar access token |
| GET | `/api/auth/me` | Sí | Usuario actual |
| POST | `/api/etl/seeding/start` | Admin | Lanzar seeding |
| POST | `/api/etl/sync/start` | Admin | Lanzar sync |
| POST | `/api/etl/execution/{id}/stop` | Admin | Detener ejecución |
| GET | `/api/etl/execution/{id}` | Sí | Estado de ejecución |
| GET | `/api/etl/executions` | Sí | Lista de ejecuciones |
| GET | `/api/etl/statistics` | Sí | Estadísticas generales |
| GET | `/api/etl/sync-control` | Sí | Estado de sync por entidad |
| WS | `/api/etl/ws` | No* | WebSocket para updates en tiempo real |

*El WebSocket NO requiere autenticación para simplificar. En producción, considerar implementar autenticación WS.

**Servicios:**
- `etl_service.py` - Lógica de gestión de procesos ETL
  - Lanzar procesos en background (subprocess)
  - Monitorear estado en tiempo real
  - Consultar estadísticas de BD

---

## 📦 Paquete Compartido: bdns_core

### Módulos

**1. DB (`bdns_core.db`)**
- `models.py` - Modelos de negocio (Beneficiario, Convocatoria, Concesion, etc.)
- `etl_models.py` - Modelos de control ETL (EtlJob, EtlExecution, SyncControl)
- `session.py` - Gestión de sesiones SQLAlchemy
- `base.py` - Base declarativa

**2. Auth (`bdns_core.auth`)** - NUEVO
- `jwt_auth.py` - Sistema de autenticación JWT
  - `create_access_token()` - Crear access token
  - `create_refresh_token()` - Crear refresh token
  - `verify_token()` - Verificar y decodificar token
  - `verify_password()` - Verificar password con bcrypt
  - `get_password_hash()` - Hashear password

**3. Business (`bdns_core.business`)**
- Lógica de negocio compartida
- Utilidades de transformación

### Instalación

Ambos backends tienen `bdns-core` como dependencia:

```toml
# apps/bdns-search-backend/pyproject.toml
dependencies = [
    "bdns-core",  # Desde packages/
    ...
]

# apps/etl-admin-backend/pyproject.toml
dependencies = [
    "bdns-core",  # Desde packages/
    ...
]
```

---

## 🚀 Cómo Ejecutar

### Requisitos Previos
```bash
# PostgreSQL 16
# Redis
# Python 3.12+
# Node.js 20+
```

### 1. Backend de Búsqueda (GraphQL)
```bash
cd apps/bdns-search-backend
pip install -e .
pip install -e ../../packages/bdns_core

# Ejecutar migraciones
alembic upgrade head

# Iniciar servidor
python main.py
# http://localhost:8000/graphql
```

### 2. Backend de ETL Admin (REST)
```bash
cd apps/etl-admin-backend
pip install -e .
pip install -e ../../packages/bdns_core

# Iniciar servidor
python main.py
# http://localhost:8001/docs
```

### 3. Frontend de Búsqueda
```bash
cd apps/bdns-search-frontend
npm install
npm run dev
# http://localhost:3000
```

### 4. Frontend de ETL Admin
```bash
cd apps/etl-admin-frontend
npm install
npm run dev
# http://localhost:3001
```

### 5. Base de Datos
```bash
# Asegurar que PostgreSQL está corriendo
docker-compose up -d postgres redis

# Aplicar migraciones
cd apps/bdns-search-backend
alembic upgrade head
```

---

## 🔄 Flujo de Trabajo ETL

```
┌──────────────────┐
│  Admin Frontend  │ (Vue3 - Puerto 3001)
│  - Login         │
│  - Dashboard     │
│  - Launch Seeding│
└────────┬─────────┘
         │
         │ POST /api/etl/seeding/start
         │ Authorization: Bearer <admin_token>
         ▼
┌──────────────────┐
│ ETL Admin API    │ (FastAPI - Puerto 8001)
│  - Verify JWT    │
│  - Check role=admin
│  - Start process │
└────────┬─────────┘
         │
         │ subprocess.Popen()
         ▼
┌──────────────────┐
│   ETL Scripts    │ (Python - apps/ETL/)
│  - Extract       │
│  - Transform     │
│  - Load          │
└────────┬─────────┘
         │
         │ INSERT INTO
         ▼
┌──────────────────┐
│   PostgreSQL     │
│  - concesion     │
│  - convocatoria  │
│  - beneficiario  │
└────────┬─────────┘
         │
         │ SELECT
         ▼
┌──────────────────┐
│ Search API       │ (GraphQL - Puerto 8000)
│  - Query data    │
│  - Cache Redis   │
└────────┬─────────┘
         │
         │ GraphQL query
         ▼
┌──────────────────┐
│ Search Frontend  │ (Vue3 - Puerto 3000)
│  - Display data  │
└──────────────────┘
```

---

## 🔒 Seguridad

### Separación de Responsabilidades

**BDNS Search (Público):**
- ✅ Sin autenticación requerida
- ✅ Solo lectura de datos
- ✅ Puede estar en internet público
- ✅ Caché agresiva (Redis)

**ETL Admin (Interno):**
- 🔐 Autenticación JWT obligatoria
- 🔐 Rol `admin` para modificaciones
- 🔐 Debe estar en red interna / VPN
- 🔐 CORS configurado para orígenes específicos

### Recomendaciones de Producción

1. **Variables de entorno:**
   ```bash
   # .env para etl-admin-backend
   JWT_SECRET_KEY=<clave-generada-con-openssl>
   DATABASE_URL=postgresql://...
   REDIS_URL=redis://...
   CORS_ORIGINS=https://admin.bdns.example.com
   ```

2. **HTTPS:**
   - Usar certificados SSL para ambos backends
   - Configurar reverse proxy (Nginx/Traefik)

3. **Rate Limiting:**
   - Limitar requests al API de ETL Admin
   - Implementar con `slowapi` o Nginx

4. **Usuarios:**
   - Migrar de hardcoded a base de datos
   - Implementar tabla `users` con roles
   - Hashear passwords en BD

5. **Logs y Auditoría:**
   - Registrar todas las acciones de ETL
   - Alertas de Telegram para fallos
   - Logs estructurados (JSON)

---

## 📊 Tabla Comparativa

| Característica | BDNS Search | ETL Admin |
|---------------|-------------|-----------|
| **Propósito** | Consulta pública | Administración interna |
| **Frontend** | Vue3 + GraphQL | Vue3 + REST + WS |
| **Backend** | FastAPI + Strawberry | FastAPI |
| **Puerto Front** | 3000 | 3001 |
| **Puerto Back** | 8000 | 8001 |
| **Autenticación** | No | Sí (JWT) |
| **Usuarios** | Público | Admin, User |
| **Cache** | Redis (1h) | No |
| **WebSocket** | No | Sí (updates ETL) |
| **Base de datos** | PostgreSQL (read-only) | PostgreSQL (read-write) |
| **Despliegue** | Internet público | Red interna |

---

## 🗂️ Archivos Creados/Modificados

### Creados

#### Backend ETL Admin
- `apps/etl-admin-backend/main.py`
- `apps/etl-admin-backend/pyproject.toml`
- `apps/etl-admin-backend/src/etl_admin/__init__.py`
- `apps/etl-admin-backend/src/etl_admin/api/__init__.py`
- `apps/etl-admin-backend/src/etl_admin/api/auth.py`
- `apps/etl-admin-backend/src/etl_admin/api/etl_router.py`
- `apps/etl-admin-backend/src/etl_admin/services/__init__.py`
- `apps/etl-admin-backend/src/etl_admin/services/etl_service.py`

#### Frontend ETL Admin
- `apps/etl-admin-frontend/package.json`
- `apps/etl-admin-frontend/vite.config.js`
- `apps/etl-admin-frontend/index.html`
- `apps/etl-admin-frontend/tailwind.config.js`
- `apps/etl-admin-frontend/postcss.config.js`
- `apps/etl-admin-frontend/src/main.js`
- `apps/etl-admin-frontend/src/App.vue`
- `apps/etl-admin-frontend/src/style.css`
- `apps/etl-admin-frontend/src/router/index.js`
- `apps/etl-admin-frontend/src/stores/auth.js`
- `apps/etl-admin-frontend/src/views/LoginView.vue`
- `apps/etl-admin-frontend/src/views/DashboardView.vue`
- `apps/etl-admin-frontend/src/views/SeedingView.vue`
- `apps/etl-admin-frontend/src/views/SyncView.vue`
- `apps/etl-admin-frontend/src/views/ExecutionsView.vue`

#### Sistema de Autenticación (bdns_core)
- `packages/bdns_core/src/bdns_core/auth/__init__.py`
- `packages/bdns_core/src/bdns_core/auth/jwt_auth.py`

#### Documentación
- `docs/SEPARACION_PROYECTOS.md` (este archivo)

### Modificados

- `packages/bdns_core/pyproject.toml` - Añadidas dependencias de auth
- `apps/backend/` → renombrado a `apps/bdns-search-backend/`
- `apps/frontend/` → renombrado a `apps/bdns-search-frontend/`

### Compartidos (sin cambios)

- `apps/ETL/` - Scripts ETL usados por ambos proyectos
- `packages/bdns_core/src/bdns_core/db/` - Modelos de datos
- `data/` - CSVs de seed

---

## 🧪 Próximos Pasos

### Corto Plazo (Sprint 1-2)

- [ ] Completar vistas de Seeding y Sync en frontend
- [ ] Implementar formularios para lanzar procesos ETL
- [ ] Añadir visualización de progreso en tiempo real
- [ ] Añadir componentes de gráficos (Chart.js)
- [ ] Tests unitarios para autenticación JWT

### Medio Plazo (Sprint 3-4)

- [ ] Migrar usuarios hardcoded a base de datos
- [ ] Implementar gestión de usuarios (CRUD)
- [ ] Añadir logs de auditoría
- [ ] Implementar notificaciones de Telegram para fallos ETL
- [ ] Configurar CI/CD separado para cada app

### Largo Plazo (Q2 2026)

- [ ] Deploy en producción con Docker
- [ ] Configurar monitoreo (Grafana + Prometheus)
- [ ] Implementar rate limiting
- [ ] Añadir autenticación OAuth (opcional)
- [ ] Documentar API con OpenAPI 3.0

---

## 📚 Referencias

- [FastAPI Docs](https://fastapi.tiangolo.com/)
- [Vue3 Docs](https://vuejs.org/)
- [Pinia Docs](https://pinia.vuejs.org/)
- [JWT.io](https://jwt.io/)
- [Python-JOSE](https://python-jose.readthedocs.io/)
- [Passlib](https://passlib.readthedocs.io/)

---

## 👥 Contacto y Soporte

Para preguntas sobre la arquitectura, consultar:
- Documentación técnica en `/docs`
- Issues en GitHub
- Equipo de desarrollo BDNS

---

**Última actualización:** 2026-02-08
**Versión:** 1.0.0
