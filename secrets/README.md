# Docker Secrets - Credenciales de Autenticación

Este directorio contiene las credenciales de usuario como Docker secrets.

## ⚠️ IMPORTANTE: SEGURIDAD

**NUNCA subas los archivos `.txt` a Git.** Están excluidos en `.gitignore`.

## Archivos de Secrets

Los secrets se leen desde archivos de texto plano:

### Obligatorios (Admin)
- `admin_username.txt` - Nombre de usuario administrador
- `admin_password_hash.txt` - Hash bcrypt de la contraseña del admin

### Opcionales (Usuario normal)
- `user_username.txt` - Nombre de usuario normal (opcional)
- `user_password_hash.txt` - Hash bcrypt de la contraseña del usuario (opcional)

## Generar Password Hash

Para generar un nuevo hash de contraseña:

```bash
cd secrets/
python3 generate_password_hash.py
```

Ingresa tu contraseña y copia el hash generado al archivo correspondiente.

## Configuración de Desarrollo

Para desarrollo local **sin Docker**, usa variables de entorno en lugar de archivos:

```bash
export ADMIN_USERNAME="admin"
export ADMIN_PASSWORD_HASH="$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LewY5GyYzpLaOvzK6"
```

## Configuración en Docker

Los secrets se montan en `/run/secrets/` dentro del contenedor:

```yaml
secrets:
  admin_username:
    file: ./secrets/admin_username.txt
  admin_password_hash:
    file: ./secrets/admin_password_hash.txt
```

El backend lee automáticamente desde estos archivos.

## Ejemplo de Configuración

### 1. Crear credenciales de admin

```bash
echo "mi_admin_user" > admin_username.txt
python3 generate_password_hash.py  # Ingresa tu password
# Copia el hash generado a admin_password_hash.txt
```

### 2. Crear credenciales de usuario normal (opcional)

```bash
echo "usuario_normal" > user_username.txt
python3 generate_password_hash.py  # Ingresa password de usuario
# Copia el hash a user_password_hash.txt
```

### 3. Iniciar Docker Compose

```bash
cd ..
docker-compose up -d backend
```

## Hash de Desarrollo (NO USAR EN PRODUCCIÓN)

Credenciales por defecto (solo para desarrollo):

- **Username**: `admin`
- **Password**: `admin123`
- **Hash**: `$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LewY5GyYzpLaOvzK6`

## Verificar que funciona

Prueba el login:

```bash
curl -X POST http://localhost:8001/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{"username": "admin", "password": "tu_password_aqui"}'
```

Deberías recibir un `access_token` y `refresh_token`.
