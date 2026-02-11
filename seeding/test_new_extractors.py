"""
Script de prueba para verificar los nuevos extractores de:
- Minimis
- Ayudas de Estado
- Partidos Políticos

Realiza consultas de prueba para verificar que los endpoints funcionan correctamente.
"""

import sys
from pathlib import Path

# Añadir paths
sys.path.insert(0, str(Path(__file__).parent / "minimis"))
sys.path.insert(0, str(Path(__file__).parent / "ayudas_estado"))
sys.path.insert(0, str(Path(__file__).parent / "partidos_politicos"))

from extract_minimis import MinimisExtractor
from extract_ayudas_estado import AyudasEstadoExtractor
from extract_partidos_politicos import PartidosPoliticosExtractor


def test_partidos_politicos():
    """Prueba el extractor de partidos políticos."""
    print("\n" + "=" * 60)
    print("PRUEBA: Partidos Políticos")
    print("=" * 60)

    extractor = PartidosPoliticosExtractor()

    try:
        # Extraer solo 2022 para prueba (año del curl del usuario)
        data = extractor.extract_partidos_by_year(2022)

        print(f"\n✅ Extracción exitosa:")
        print(f"   - Total registros: {len(data)}")

        if data:
            print(f"\n📋 Muestra del primer registro:")
            first = data[0]
            for key, value in list(first.items())[:5]:
                print(f"   - {key}: {value}")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()


def test_minimis():
    """Prueba el extractor de minimis."""
    print("\n" + "=" * 60)
    print("PRUEBA: Ayudas de Minimis")
    print("=" * 60)

    extractor = MinimisExtractor()

    try:
        # Extraer año reciente para prueba
        data = extractor.extract_minimis_by_year(2024)

        print(f"\n✅ Extracción exitosa:")
        print(f"   - Total registros: {len(data)}")

        if data:
            print(f"\n📋 Muestra del primer registro:")
            first = data[0]
            for key, value in list(first.items())[:5]:
                print(f"   - {key}: {value}")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        print(f"\nℹ️  Nota: El endpoint de minimis puede requerir ajustes")
        print(f"   según la documentación oficial de la API BDNS")


def test_ayudas_estado():
    """Prueba el extractor de ayudas de estado."""
    print("\n" + "=" * 60)
    print("PRUEBA: Ayudas de Estado")
    print("=" * 60)

    extractor = AyudasEstadoExtractor()

    try:
        # Extraer año reciente para prueba
        data = extractor.extract_ayudas_by_year(2024)

        print(f"\n✅ Extracción exitosa:")
        print(f"   - Total registros: {len(data)}")

        if data:
            print(f"\n📋 Muestra del primer registro:")
            first = data[0]
            for key, value in list(first.items())[:5]:
                print(f"   - {key}: {value}")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        print(f"\nℹ️  Nota: El endpoint de ayudas de estado puede requerir ajustes")
        print(f"   según la documentación oficial de la API BDNS")


def main():
    """Ejecuta todas las pruebas."""
    print("\n" + "=" * 60)
    print("VERIFICACIÓN DE NUEVOS EXTRACTORES BDNS")
    print("=" * 60)
    print("\nProbando conectividad con los endpoints de:")
    print("1. Partidos Políticos")
    print("2. Ayudas de Minimis")
    print("3. Ayudas de Estado")
    print("\nEsto puede tardar unos minutos...")

    # Probar partidos políticos (sabemos que funciona)
    test_partidos_politicos()

    # Probar minimis (puede requerir ajuste de endpoint)
    test_minimis()

    # Probar ayudas de estado (puede requerir ajuste de endpoint)
    test_ayudas_estado()

    print("\n" + "=" * 60)
    print("PRUEBAS COMPLETADAS")
    print("=" * 60)
    print("\nℹ️  Si algún extractor falla, verifica:")
    print("   - Que el endpoint exista en la API BDNS oficial")
    print("   - Los parámetros requeridos según la documentación")
    print("   - La estructura de la respuesta JSON")
    print("\n📚 Referencias:")
    print("   - Portal BDNS: https://www.pap.hacienda.gob.es/bdnstrans")
    print("   - Documentación API: Ver archivos FAQ oficiales")


if __name__ == "__main__":
    main()
