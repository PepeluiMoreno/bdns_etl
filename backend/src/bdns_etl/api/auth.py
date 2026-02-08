"""
Router de autenticación para ETL Admin.

Endpoints:
- POST /login - Login con username/password
- POST /refresh - Renovar access token con refresh token
- GET /me - Obtener usuario actual
"""
from fastapi import APIRouter, HTTPException, Depends, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel

from bdns_core.auth import (
    Token,
    UserInToken,
    create_token_pair,
    refresh_access_token,
    verify_password,
    verify_token,
)


router = APIRouter()
security = HTTPBearer()


# ==========================================
# SCHEMAS
# ==========================================

class LoginRequest(BaseModel):
    """Request de login."""
    username: str
    password: str


class RefreshRequest(BaseModel):
    """Request para refresh token."""
    refresh_token: str


class UserResponse(BaseModel):
    """Response con datos de usuario."""
    username: str
    role: str


# ==========================================
# USUARIOS HARDCODED (temporal)
# TODO: Mover a base de datos
# ==========================================

# Passwords hasheados con bcrypt
# admin: "admin123"
# user: "user123"
FAKE_USERS_DB = {
    "admin": {
        "username": "admin",
        "hashed_password": "$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LewY5GyYzpLaOvzK6",  # admin123
        "role": "admin"
    },
    "user": {
        "username": "user",
        "hashed_password": "$2b$12$gPuKVw5CQXQY7kv5QfH.SeMKfV6yVqH8p3yH8KfV6yVqH8p3yH8Kf",  # user123
        "role": "user"
    }
}


# ==========================================
# DEPENDENCIAS
# ==========================================

async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)) -> UserInToken:
    """
    Dependency para obtener usuario actual desde JWT token.

    Extrae el token del header Authorization: Bearer <token>
    y verifica su validez.
    """
    token = credentials.credentials
    user = verify_token(token)

    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token inválido o expirado",
            headers={"WWW-Authenticate": "Bearer"},
        )

    return user


async def require_admin(current_user: UserInToken = Depends(get_current_user)) -> UserInToken:
    """
    Dependency para requerir rol de admin.

    Lanza excepción si el usuario no es admin.
    """
    if current_user.role != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Acceso denegado: se requiere rol de admin"
        )
    return current_user


# ==========================================
# ENDPOINTS
# ==========================================

@router.post("/login", response_model=Token)
async def login(request: LoginRequest):
    """
    Login con username y password.

    Retorna access_token y refresh_token si las credenciales son correctas.
    """
    # Buscar usuario
    user = FAKE_USERS_DB.get(request.username)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Usuario o contraseña incorrectos"
        )

    # Verificar password
    if not verify_password(request.password, user["hashed_password"]):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Usuario o contraseña incorrectos"
        )

    # Crear tokens
    tokens = create_token_pair(
        username=user["username"],
        role=user["role"]
    )

    return tokens


@router.post("/refresh")
async def refresh(request: RefreshRequest):
    """
    Renueva el access token usando un refresh token válido.

    El refresh token debe ser el obtenido en el login.
    """
    new_access_token = refresh_access_token(request.refresh_token)

    if new_access_token is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Refresh token inválido o expirado"
        )

    return {
        "access_token": new_access_token,
        "token_type": "bearer"
    }


@router.get("/me", response_model=UserResponse)
async def get_me(current_user: UserInToken = Depends(get_current_user)):
    """
    Obtiene información del usuario actual.

    Requiere autenticación (token JWT en header).
    """
    return UserResponse(
        username=current_user.username,
        role=current_user.role
    )


@router.get("/verify")
async def verify_auth(current_user: UserInToken = Depends(get_current_user)):
    """
    Verifica que el token sea válido.

    Endpoint útil para el frontend para verificar autenticación.
    """
    return {
        "valid": True,
        "username": current_user.username,
        "role": current_user.role
    }
