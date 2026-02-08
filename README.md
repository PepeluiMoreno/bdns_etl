# BDNS ETL

Sistema de administración ETL para la Base de Datos Nacional de Subvenciones (BDNS).

## 🎯 Descripción

Aplicación administrativa para gestionar procesos de extracción, transformación y carga (ETL) de datos desde la API oficial de BDNS hacia la base de datos PostgreSQL.

**Modo de operación:** Lectura/escritura en BD (actualiza datos que consume `bdns_portal`)

## 🏗️ Arquitectura

```
bdns_etl/
├── seeding/           # Scripts de carga inicial
│   ├── concesiones/
│   ├── convocatorias/
│   ├── beneficiarios/
│   └── catalogos/
├── sync/              # Scripts de sincronización
├── backend/           # API FastAPI + gestión ETL
│   ├── src/bdns_etl/
│   └── main.py
└── frontend/          # UI Vue3 admin
    ├── src/
    └── package.json
```

## 🚀 Stack Tecnológico

### Backend
- FastAPI + REST API
- WebSocket (updates en tiempo real)
- JWT Authentication
- Puerto: 8001

### Frontend
- Vue 3 + Vite
- Pinia (state management)
- TailwindCSS
- Puerto: 3001

### ETL Scripts
- Python 3.12+
- Requests (API BDNS)
- SQLAlchemy (DB)

## 🔧 Instalación Rápida

### Backend
```bash
cd backend
python -m venv .venv
source .venv/bin/activate
pip install -e .
python main.py
```

### Frontend
```bash
cd frontend
npm install
npm run dev
```

### Ejecutar Seeding
```bash
cd seeding
python -m concesiones.extract.extract_concesiones --year 2024
python -m concesiones.transform.transform_concesiones --year 2024
python -m concesiones.load.load_concesiones --year 2024
```

## 🔐 Autenticación

**Usuario admin:**
- Username: `admin`
- Password: `admin123`

**Usuario normal:**
- Username: `user`
- Password: `user123`

## 🔗 Enlaces

- **Backend API:** http://localhost:8001/docs
- **Frontend:** http://localhost:3001
- **WebSocket:** ws://localhost:8001/api/etl/ws

## 📝 Notas

- **Autenticación:** JWT requerida
- **Roles:** admin (modificar), user (solo lectura)
- **BD:** PostgreSQL compartida con `bdns_portal`
- **Modo:** Lectura y escritura

---

**Versión:** 1.0.0
