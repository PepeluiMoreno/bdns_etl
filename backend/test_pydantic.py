#!/usr/bin/env python
"""Test directo de Pydantic Settings."""
import os
import sys
from pathlib import Path
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

os.chdir(Path(__file__).parent)

print(f"Working directory: {os.getcwd()}")
print(f".env.development exists: {Path('.env.development').exists()}")
print()

# Test 1: Settings simple
class SimpleSettings(BaseSettings):
    PORT: int = 8001
    DEBUG: bool = False
    DATABASE_URL: str = "default_db"

    model_config = SettingsConfigDict(
        env_file=".env.development" if os.getenv("ENVIRONMENT", "development") == "development" else ".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
        extra="ignore",
    )

simple = SimpleSettings()
print("SimpleSettings:")
print(f"  PORT: {simple.PORT}")
print(f"  DEBUG: {simple.DEBUG}")
print(f"  DATABASE_URL: {simple.DATABASE_URL}")
print()

# Test 2: Con Field
class FieldSettings(BaseSettings):
    PORT: int = Field(default=8001)
    DEBUG: bool = Field(default=False)
    DATABASE_URL: str = Field(default="default_db")

    model_config = SettingsConfigDict(
        env_file=".env.development",
        env_file_encoding="utf-8",
        case_sensitive=True,
        extra="ignore",
    )

field = FieldSettings()
print("FieldSettings:")
print(f"  PORT: {field.PORT}")
print(f"  DEBUG: {field.DEBUG}")
print(f"  DATABASE_URL: {field.DATABASE_URL}")
