"""
Servicio para leer credenciales desde Docker secrets.

En Docker Compose, los secrets se montan en /run/secrets/
En desarrollo local, se leen desde variables de entorno.
"""
import os
from pathlib import Path
from typing import Optional


class SecretsManager:
    """Gestor de secrets para credenciales de autenticación."""

    SECRETS_DIR = Path("/run/secrets")

    @classmethod
    def read_secret(cls, secret_name: str) -> Optional[str]:
        """
        Lee un secret desde Docker o variable de entorno.

        Prioridad:
        1. Docker secret en /run/secrets/{secret_name}
        2. Variable de entorno {secret_name.upper()}
        3. None si no existe

        Args:
            secret_name: Nombre del secret (ej: "admin_username")

        Returns:
            Contenido del secret o None
        """
        # Intentar leer desde Docker secret
        secret_path = cls.SECRETS_DIR / secret_name
        if secret_path.exists():
            return secret_path.read_text().strip()

        # Fallback: variable de entorno
        env_var = secret_name.upper()
        return os.getenv(env_var)

    @classmethod
    def get_admin_credentials(cls) -> tuple[str, str]:
        """
        Obtiene credenciales de administrador.

        Returns:
            (username, hashed_password) tupla

        Raises:
            ValueError: Si no se encuentran las credenciales
        """
        username = cls.read_secret("admin_username")
        password_hash = cls.read_secret("admin_password_hash")

        if not username or not password_hash:
            raise ValueError(
                "Credenciales de admin no configuradas. "
                "Configura admin_username y admin_password_hash en Docker secrets "
                "o variables de entorno."
            )

        return username, password_hash

    @classmethod
    def get_user_credentials(cls) -> Optional[tuple[str, str]]:
        """
        Obtiene credenciales de usuario normal (opcional).

        Returns:
            (username, hashed_password) tupla o None si no está configurado
        """
        username = cls.read_secret("user_username")
        password_hash = cls.read_secret("user_password_hash")

        if username and password_hash:
            return username, password_hash

        return None


# Singleton
secrets_manager = SecretsManager()
