-- ============================================================
-- Script de Inicialización de Base de Datos BDNS
-- Crea schemas, extensiones, usuarios y permisos
-- ============================================================

-- 1. Crear schemas primero (algunas extensiones los necesitan)
CREATE SCHEMA IF NOT EXISTS bdns;
CREATE SCHEMA IF NOT EXISTS etl_admin;

-- Comentarios descriptivos
COMMENT ON SCHEMA bdns IS 'Datos del dominio BDNS: convocatorias, concesiones, beneficiarios y catálogos';
COMMENT ON SCHEMA etl_admin IS 'Infraestructura ETL: control de ejecuciones, jobs y sincronización';

-- 2. Extensiones (antes de tablas, pueden ser necesarias para tipos/índices)

-- uuid-ossp: para gen_random_uuid() si no usas pg_uuidv7
CREATE EXTENSION IF NOT EXISTS "uuid-ossp" SCHEMA public;
-- Si tienes pg_uuidv7 disponible (mejor para UUID v7 time-ordered)
-- CREATE EXTENSION IF NOT EXISTS "pg_uuidv7" SCHEMA public;

-- pg_trgm: búsquedas textuales tipo LIKE rápidas
CREATE EXTENSION IF NOT EXISTS "pg_trgm" SCHEMA public;

-- unaccent: búsquedas sin tildes
CREATE EXTENSION IF NOT EXISTS "unaccent" SCHEMA public;

-- postgis: datos geográficos (si los usas)
CREATE EXTENSION IF NOT EXISTS "postgis" SCHEMA public;



-- 3. Permisos para usuario bdns
GRANT USAGE ON SCHEMA bdns TO bdns;
GRANT USAGE ON SCHEMA etl_admin TO bdns;
GRANT USAGE ON SCHEMA public TO bdns;

-- Permisos en tablas existentes
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA bdns TO bdns;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA etl_admin TO bdns;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO bdns;

-- Permisos en tablas futuras
ALTER DEFAULT PRIVILEGES IN SCHEMA bdns GRANT ALL ON TABLES TO bdns;
ALTER DEFAULT PRIVILEGES IN SCHEMA etl_admin GRANT ALL ON TABLES TO bdns;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO bdns;

-- Permisos en secuencias
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA bdns TO bdns;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA etl_admin TO bdns;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO bdns;

ALTER DEFAULT PRIVILEGES IN SCHEMA bdns GRANT ALL ON SEQUENCES TO bdns;
ALTER DEFAULT PRIVILEGES IN SCHEMA etl_admin GRANT ALL ON SEQUENCES TO bdns;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO bdns;

-- Permisos en funciones (para extensiones)
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO bdns;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT EXECUTE ON FUNCTIONS TO bdns;

-- Verificación
SELECT
    nspname as schema_name,
    pg_catalog.pg_get_userbyid(nspowner) as owner
FROM pg_catalog.pg_namespace
WHERE nspname IN ('bdns', 'etl_admin', 'public')
ORDER BY nspname;
