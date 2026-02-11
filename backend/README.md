# BDNS ETL Admin Backend

Backend FastAPI para administración de procesos ETL de BDNS.

## 🚀 Inicio Rápido

### Opción 1: Docker Compose (Recomendado)

```bash
cd /home/jose/dev/bdns/bdns_etl
docker compose up -d backend
```

El backend estará disponible en: http://localhost:8001

### Opción 2: Ejecución Local

```bash
cd backend

# Activar entorno virtual
source .venv/bin/activate

# Configurar variables de entorno
export ADMIN_USERNAME="admin"
export ADMIN_PASSWORD_HASH="$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LewY5GyYzpLaOvzK6"
export DATABASE_URL="postgresql://bdns_user:bdns_password@localhost:5432/bdns"
export JWT_SECRET_KEY="tu-secret-key-aqui"

# Ejecutar servidor
python main.py
```

## 📝 API Endpoints

### Autenticación

- `POST /api/auth/login` - Login con username/password
- `POST /api/auth/refresh` - Renovar access token
- `GET /api/auth/me` - Obtener usuario actual
- `GET /api/auth/verify` - Verificar token

### ETL Operations (requiere auth)

- `POST /api/etl/seeding/start` - Iniciar proceso de seeding (requiere admin)
- `POST /api/etl/sync/start` - Iniciar sincronización (requiere admin)
- `GET /api/etl/execution/{id}` - Obtener estado de ejecución
- `GET /api/etl/executions` - Listar ejecuciones recientes
- `GET /api/etl/statistics` - Obtener estadísticas
- `POST /api/etl/execution/{id}/stop` - Detener ejecución (requiere admin)

### WebSocket

- `WS /api/etl/ws` - WebSocket para actualizaciones en tiempo real

## 🔐 Autenticación con Docker Secrets

Las credenciales se leen desde Docker secrets en producción:

### Configurar Secrets

1. Generar password hash:
```bash
cd ../secrets/
python3 generate_password_hash.py
```

2. Guardar credenciales:
```bash
echo "mi_usuario" > admin_username.txt
echo "$2b$12$..." > admin_password_hash.txt
```

3. Los secrets se montan automáticamente en `/run/secrets/` en Docker

### Desarrollo Local (sin Docker)

Usa variables de entorno:
```bash
export ADMIN_USERNAME="admin"
export ADMIN_PASSWORD_HASH="$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LewY5GyYzpLaOvzK6"
```

## 🧪 Probar la API

### Login
```bash
curl -X POST http://localhost:8001/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{"username": "admin", "password": "admin123"}'
```

Respuesta:
```json
{
  "access_token": "eyJ0eXAiOiJKV1QiLCJhbGc...",
  "refresh_token": "eyJ0eXAiOiJKV1QiLCJhbGc...",
  "token_type": "bearer"
}
```

### Usar token para llamadas autenticadas
```bash
TOKEN="tu_access_token_aqui"

curl -X GET http://localhost:8001/api/etl/statistics \
  -H "Authorization: Bearer $TOKEN"
```

### Iniciar proceso de seeding
```bash
curl -X POST http://localhost:8001/api/etl/seeding/start \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "year": 2026,
    "entity": "all",
    "batch_size": 5000
  }'
```

## 📚 Documentación Interactiva

- **Swagger UI**: http://localhost:8001/docs
- **ReDoc**: http://localhost:8001/redoc

## 🔧 Desarrollo

### Instalar dependencias
```bash
cd backend
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

### Variables de Entorno

Crear archivo `.env`:
```env
DATABASE_URL=postgresql://bdns_user:bdns_password@localhost:5432/bdns
JWT_SECRET_KEY=tu-secret-key-super-secreto
ADMIN_USERNAME=admin
ADMIN_PASSWORD_HASH=$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LewY5GyYzpLaOvzK6
```

## 🐳 Docker

### Ver logs
```bash
docker compose logs -f backend
```

### Reiniciar backend
```bash
docker compose restart backend
```

### Reconstruir imagen
```bash
docker compose up -d --build backend
```

## 🛠️ Estructura del Código

```
backend/
├── src/
│   └── bdns_etl/
│       ├── api/
│       │   ├── auth.py          # Autenticación JWT
│       │   └── etl_router.py    # Endpoints ETL
│       └── services/
│           ├── secrets.py       # Gestión de secrets
│           └── etl_service.py   # Lógica de negocio ETL
├── main.py                      # Punto de entrada FastAPI
├── pyproject.toml               # Configuración del proyecto
└── Dockerfile                   # Imagen Docker
```

## 📦 Dependencias

- **FastAPI** - Framework web
- **Uvicorn** - Servidor ASGI
- **Pydantic** - Validación de datos
- **SQLAlchemy** - ORM para PostgreSQL
- **python-jose** - JWT tokens
- **passlib** - Hashing de passwords
- **bdns-core** - Lógica compartida (opcional)

## ⚙️ Configuración

### Puerto
Por defecto: `8001`

### CORS
Configurado para:
- http://localhost:3001 (ETL Admin Frontend)
- http://localhost:3000 (BDNS Search Frontend)

### Base de Datos
PostgreSQL 16 con PostGIS
- Host: localhost (o 'postgres' en Docker)
- Puerto: 5432
- Database: bdns

## 🔒 Seguridad

- ✅ Autenticación JWT con tokens de refresh
- ✅ Passwords hasheados con bcrypt
- ✅ Secrets gestionados vía Docker secrets
- ✅ CORS configurado explícitamente
- ✅ Validación de datos con Pydantic
- ✅ Roles de usuario (admin/user)

## 🚦 Health Check

```bash
curl http://localhost:8001/health
```

Respuesta:
```json
{
  "status": "ok",
  "service": "etl-admin-backend"
}
```

## 📞 Soporte

Para problemas o preguntas sobre el backend, revisa:
1. Logs: `docker compose logs backend`
2. Documentación API: http://localhost:8001/docs
3. Variables de entorno en `.env`
4. Secrets en `../secrets/`
