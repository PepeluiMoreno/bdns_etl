-- ============================================================
-- Script de Inicialización de Base de Datos BDNS
-- Crea schemas, usuarios y permisos
-- ============================================================

-- Crear schemas si no existen
CREATE SCHEMA IF NOT EXISTS public;
CREATE SCHEMA IF NOT EXISTS etl_admin;

-- Comentarios descriptivos
COMMENT ON SCHEMA public IS 'Datos del dominio BDNS: convocatorias, concesiones, beneficiarios y catálogos';
COMMENT ON SCHEMA etl_admin IS 'Infraestructura ETL: control de ejecuciones, jobs y sincronización';

-- Permisos para usuario bdns (ya existe)
GRANT USAGE ON SCHEMA public TO bdns;
GRANT USAGE ON SCHEMA etl_admin TO bdns;

-- Permisos completos en todas las tablas de ambos schemas
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO bdns;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA etl_admin TO bdns;

-- Permisos en tablas futuras
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO bdns;
ALTER DEFAULT PRIVILEGES IN SCHEMA etl_admin GRANT ALL ON TABLES TO bdns;

-- Permisos en secuencias (para IDs autoincrementales)
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO bdns;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA etl_admin TO bdns;

ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO bdns;
ALTER DEFAULT PRIVILEGES IN SCHEMA etl_admin GRANT ALL ON SEQUENCES TO bdns;

-- Habilitar extensiones necesarias
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";
CREATE EXTENSION IF NOT EXISTS "unaccent";
CREATE EXTENSION IF NOT EXISTS "postgis";

-- Verificación
SELECT
    nspname as schema_name,
    pg_catalog.pg_get_userbyid(nspowner) as owner
FROM pg_catalog.pg_namespace
WHERE nspname IN ('public', 'etl_admin')
ORDER BY nspname;
