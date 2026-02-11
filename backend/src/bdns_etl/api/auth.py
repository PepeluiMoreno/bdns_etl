"""
Router de autenticación para ETL Admin.

Usa autenticación centralizada de bdns_core con base de datos.

Endpoints:
- POST /login - Login con username/password
- POST /refresh - Renovar access token con refresh token
- GET /me - Obtener usuario actual
- GET /verify - Verificar token
"""
from fastapi import APIRouter, HTTPException, Depends, status
from sqlalchemy.orm import Session

# Importar TODO desde bdns_core (ya no hay duplicación)
from bdns_core.auth import (
    LoginRequest,
    Token,
    TokenRefresh,
    UserInToken,
    UserResponse,
    create_token_pair,
    refresh_access_token,
    verify_password,
    verify_token,
    get_current_user,
    require_admin,
    UserService,
)
from bdns_core.db.session import get_db


router = APIRouter()


# ==========================================
# ENDPOINTS
# ==========================================

@router.post("/login", response_model=Token)
async def login(request: LoginRequest, db: Session = Depends(get_db)):
    """
    Login con username y password.

    Retorna access_token y refresh_token si las credenciales son correctas.
    Ahora usa base de datos en lugar de memoria.
    """
    # DEBUG: Log de credenciales recibidas
    print(f"🔐 Login attempt: username='{request.username}', password_length={len(request.password)}")

    # Autenticar usuario contra base de datos
    user = UserService.authenticate_user(db, request.username, request.password)

    # DEBUG: Log resultado
    print(f"🔐 Auth result: {'SUCCESS' if user else 'FAILED'}")

    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Usuario o contraseña incorrectos",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # Crear tokens
    tokens = create_token_pair(
        username=user.username,
        role=user.role
    )

    return tokens


@router.post("/refresh")
async def refresh(request: TokenRefresh):
    """
    Renueva el access token usando un refresh token válido.

    El refresh token debe ser el obtenido en el login.
    """
    new_access_token = refresh_access_token(request.refresh_token)

    if new_access_token is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Refresh token inválido o expirado",
            headers={"WWW-Authenticate": "Bearer"},
        )

    return {
        "access_token": new_access_token,
        "token_type": "bearer"
    }


@router.get("/me", response_model=UserResponse)
async def get_me(
    current_user: UserInToken = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Obtiene información del usuario actual desde la base de datos.

    Requiere autenticación (token JWT en header).
    """
    # Obtener usuario completo desde BD
    user = UserService.get_user_by_username(db, current_user.username)

    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Usuario no encontrado"
        )

    return UserResponse(
        id=str(user.id),
        username=user.username,
        email=user.email,
        nombre=user.nombre,
        role=user.role,
        activo=user.activo,
        telegram_chat_id=user.telegram_chat_id,
        telegram_username=user.telegram_username,
        telegram_verificado=user.telegram_verificado,
        created_at=user.created_at,
        updated_at=user.updated_at,
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
