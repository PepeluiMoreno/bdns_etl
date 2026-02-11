#!/usr/bin/env python3
"""
Script para generar password hashes bcrypt.

Uso:
    python generate_password_hash.py

El hash generado se puede usar en los archivos de secrets.
"""
from passlib.context import CryptContext

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def generate_hash(password: str) -> str:
    """Genera un hash bcrypt de la contraseña."""
    return pwd_context.hash(password)


if __name__ == "__main__":
    print("=== Generador de Password Hash (bcrypt) ===\n")
    password = input("Ingresa la contraseña: ")

    if not password:
        print("❌ La contraseña no puede estar vacía")
        exit(1)

    hash_value = generate_hash(password)

    print(f"\n✅ Hash generado:")
    print(f"{hash_value}")
    print(f"\n💡 Copia este valor en tu archivo de secret (ej: secrets/admin_password_hash.txt)")
