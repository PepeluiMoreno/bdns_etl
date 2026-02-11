#!/usr/bin/env python3
"""
Orquestador maestro para extracción completa de concesiones.

Extrae concesiones de las 4 fuentes oficiales de la API BDNS:
1. Concesiones ordinarias (endpoint estándar)
2. Ayudas de Minimis (retención 10 años)
3. Ayudas de Estado (retención 10 años)
4. Partidos Políticos (transparencia especial)

El resultado final es el conjunto de concesiones NO REPETIDAS,
garantizado por unique constraint en base de datos:
    (id_concesion, fecha_concesion, regimen_tipo)

Uso:
    python orchestrate_all_concesiones.py --year 2024
    python orchestrate_all_concesiones.py --year 2024 --parallel
    python orchestrate_all_concesiones.py --year 2024 --sources minimis,ayudas_estado
"""
import argparse
import asyncio
import sys
import logging
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ConcesionesOrchestrator:
    """Orquestador de extracción de concesiones desde 4 fuentes."""

    def __init__(self, year: int, output_dir: Path = None):
        """
        Inicializa el orquestador.

        Args:
            year: Año a extraer
            output_dir: Directorio de salida (default: ./data)
        """
        self.year = year
        self.output_dir = output_dir or Path(__file__).parent / "data"
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Rutas de extractores
        self.base_dir = Path(__file__).parent
        self.extractors = {
            "ordinarias": self.base_dir / "concesiones" / "extract" / "extract_concesiones.py",
            "minimis": self.base_dir / "minimis" / "extract_minimis.py",
            "ayudas_estado": self.base_dir / "ayudas_estado" / "extract_ayudas_estado.py",
            "partidos_politicos": self.base_dir / "partidos_politicos" / "extract_partidos_politicos.py",
        }

        # Resultados
        self.results: Dict[str, Dict[str, Any]] = {}

    async def extract_source(self, source_name: str) -> Dict[str, Any]:
        """
        Extrae concesiones de una fuente específica.

        Args:
            source_name: Nombre de la fuente (ordinarias, minimis, etc.)

        Returns:
            Dict con status, count y metadata
        """
        extractor_path = self.extractors[source_name]

        if not extractor_path.exists():
            logger.error(f"❌ Extractor no encontrado: {extractor_path}")
            return {
                "status": "error",
                "source": source_name,
                "error": "Extractor not found",
                "count": 0
            }

        logger.info(f"🔄 Extrayendo {source_name} para año {self.year}...")
        start_time = datetime.now()

        try:
            # Construir comando
            cmd = [
                sys.executable,
                str(extractor_path),
                "--year", str(self.year),
                "--output-dir", str(self.output_dir)
            ]

            # Ejecutar extractor
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )

            stdout, stderr = await process.communicate()

            duration = (datetime.now() - start_time).total_seconds()

            if process.returncode == 0:
                logger.info(f"✅ {source_name} completado en {duration:.1f}s")

                # Intentar contar registros del output
                output_text = stdout.decode('utf-8')
                count = self._extract_count_from_output(output_text)

                return {
                    "status": "completed",
                    "source": source_name,
                    "duration": duration,
                    "count": count,
                    "stdout": output_text[:500] if output_text else None
                }
            else:
                error_text = stderr.decode('utf-8')
                logger.error(f"❌ {source_name} falló: {error_text[:200]}")

                return {
                    "status": "error",
                    "source": source_name,
                    "duration": duration,
                    "error": error_text[:500],
                    "count": 0
                }

        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            logger.error(f"❌ Error ejecutando {source_name}: {str(e)}")

            return {
                "status": "error",
                "source": source_name,
                "duration": duration,
                "error": str(e),
                "count": 0
            }

    def _extract_count_from_output(self, output: str) -> int:
        """Intenta extraer el conteo de registros del output."""
        # Buscar patrones comunes como "Total: 1234" o "Extracted 1234 records"
        import re
        patterns = [
            r"Total[:\s]+(\d+)",
            r"Extracted[:\s]+(\d+)",
            r"(\d+)\s+records",
            r"(\d+)\s+concesiones",
        ]

        for pattern in patterns:
            match = re.search(pattern, output, re.IGNORECASE)
            if match:
                return int(match.group(1))

        return 0

    async def extract_all_parallel(self, sources: List[str] = None) -> Dict[str, Any]:
        """
        Extrae todas las fuentes en paralelo.

        Args:
            sources: Lista de fuentes específicas o None para todas

        Returns:
            Dict con resultados de todas las fuentes
        """
        sources = sources or list(self.extractors.keys())

        logger.info(f"🚀 Iniciando extracción paralela de {len(sources)} fuentes para {self.year}")
        logger.info(f"   Fuentes: {', '.join(sources)}")

        start_time = datetime.now()

        # Ejecutar todos en paralelo
        tasks = [self.extract_source(source) for source in sources]
        results = await asyncio.gather(*tasks)

        # Consolidar resultados
        total_duration = (datetime.now() - start_time).total_seconds()
        total_count = sum(r.get("count", 0) for r in results)
        completed = sum(1 for r in results if r["status"] == "completed")
        failed = len(results) - completed

        summary = {
            "year": self.year,
            "mode": "parallel",
            "total_duration": total_duration,
            "total_sources": len(sources),
            "completed": completed,
            "failed": failed,
            "total_records_extracted": total_count,
            "sources": {r["source"]: r for r in results}
        }

        self.results = summary

        # Log resumen
        logger.info(f"\n{'='*60}")
        logger.info(f"📊 RESUMEN DE EXTRACCIÓN - AÑO {self.year}")
        logger.info(f"{'='*60}")
        logger.info(f"   Duración total: {total_duration:.1f}s")
        logger.info(f"   Fuentes completadas: {completed}/{len(sources)}")
        logger.info(f"   Registros extraídos: {total_count:,}")
        logger.info("")

        for result in results:
            source = result["source"]
            status_icon = "✅" if result["status"] == "completed" else "❌"
            count = result.get("count", 0)
            duration = result.get("duration", 0)

            logger.info(f"   {status_icon} {source:20s} {count:8,} registros  ({duration:.1f}s)")

        if failed > 0:
            logger.warning(f"\n⚠️  {failed} fuente(s) fallaron - revisar logs arriba")

        logger.info(f"{'='*60}\n")

        return summary

    async def extract_all_sequential(self, sources: List[str] = None) -> Dict[str, Any]:
        """
        Extrae todas las fuentes secuencialmente.

        Args:
            sources: Lista de fuentes específicas o None para todas

        Returns:
            Dict con resultados de todas las fuentes
        """
        sources = sources or list(self.extractors.keys())

        logger.info(f"🚀 Iniciando extracción secuencial de {len(sources)} fuentes para {self.year}")

        start_time = datetime.now()
        results = []

        for source in sources:
            result = await self.extract_source(source)
            results.append(result)

            # Pausa breve entre extracciones para no saturar la API
            await asyncio.sleep(2)

        total_duration = (datetime.now() - start_time).total_seconds()
        total_count = sum(r.get("count", 0) for r in results)
        completed = sum(1 for r in results if r["status"] == "completed")
        failed = len(results) - completed

        summary = {
            "year": self.year,
            "mode": "sequential",
            "total_duration": total_duration,
            "total_sources": len(sources),
            "completed": completed,
            "failed": failed,
            "total_records_extracted": total_count,
            "sources": {r["source"]: r for r in results}
        }

        self.results = summary

        # Log resumen (igual que parallel)
        logger.info(f"\n{'='*60}")
        logger.info(f"📊 RESUMEN DE EXTRACCIÓN - AÑO {self.year}")
        logger.info(f"{'='*60}")
        logger.info(f"   Duración total: {total_duration:.1f}s")
        logger.info(f"   Fuentes completadas: {completed}/{len(sources)}")
        logger.info(f"   Registros extraídos: {total_count:,}")
        logger.info("")

        for result in results:
            source = result["source"]
            status_icon = "✅" if result["status"] == "completed" else "❌"
            count = result.get("count", 0)
            duration = result.get("duration", 0)

            logger.info(f"   {status_icon} {source:20s} {count:8,} registros  ({duration:.1f}s)")

        if failed > 0:
            logger.warning(f"\n⚠️  {failed} fuente(s) fallaron - revisar logs arriba")

        logger.info(f"{'='*60}\n")

        return summary

    def print_deduplication_info(self):
        """Imprime información sobre la deduplicación."""
        logger.info(f"\n{'='*60}")
        logger.info("🔍 INFORMACIÓN DE DEDUPLICACIÓN")
        logger.info(f"{'='*60}")
        logger.info("")
        logger.info("Las concesiones se deduplicarán automáticamente en la base de datos")
        logger.info("mediante la constraint única:")
        logger.info("")
        logger.info("    UNIQUE (id_concesion, fecha_concesion, regimen_tipo)")
        logger.info("")
        logger.info("Esto significa:")
        logger.info("  • Si una misma concesión aparece en múltiples fuentes")
        logger.info("    (ej: ordinarias Y minimis), solo se guardará UNA vez")
        logger.info("  • El campo 'regimen_tipo' identifica el origen:")
        logger.info("      - 'ordinaria': concesiones estándar")
        logger.info("      - 'minimis': ayudas de minimis")
        logger.info("      - 'ayuda_estado': ayudas de estado")
        logger.info("      - 'partidos_politicos': concesiones a partidos")
        logger.info("")
        logger.info("📌 Resultado final: conjunto de concesiones NO REPETIDAS")
        logger.info(f"{'='*60}\n")


async def main():
    """Función principal."""
    parser = argparse.ArgumentParser(
        description="Orquestador de extracción de concesiones desde 4 fuentes BDNS",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos de uso:

  # Extraer todas las fuentes en paralelo (más rápido)
  python orchestrate_all_concesiones.py --year 2024 --parallel

  # Extraer todas las fuentes secuencialmente
  python orchestrate_all_concesiones.py --year 2024

  # Extraer solo fuentes específicas
  python orchestrate_all_concesiones.py --year 2024 --sources minimis,ayudas_estado

  # Especificar directorio de salida
  python orchestrate_all_concesiones.py --year 2024 --output-dir /tmp/concesiones
        """
    )

    parser.add_argument("--year", type=int, required=True,
                       help="Año a extraer (ej: 2024)")
    parser.add_argument("--parallel", action="store_true",
                       help="Ejecutar extractores en paralelo (más rápido)")
    parser.add_argument("--sources", type=str,
                       help="Fuentes específicas separadas por coma (ej: minimis,ayudas_estado)")
    parser.add_argument("--output-dir", type=str,
                       help="Directorio de salida para JSONs (default: ./data)")

    args = parser.parse_args()

    # Parsear fuentes
    sources = None
    if args.sources:
        sources = [s.strip() for s in args.sources.split(",")]

    # Crear orquestador
    output_dir = Path(args.output_dir) if args.output_dir else None
    orchestrator = ConcesionesOrchestrator(args.year, output_dir)

    # Mostrar info de deduplicación
    orchestrator.print_deduplication_info()

    # Ejecutar extracciones
    if args.parallel:
        await orchestrator.extract_all_parallel(sources)
    else:
        await orchestrator.extract_all_sequential(sources)

    # Mensaje final
    logger.info("✅ Extracción completada")
    logger.info(f"📁 Archivos JSON guardados en: {orchestrator.output_dir}")
    logger.info("")
    logger.info("📌 Próximo paso: Cargar a base de datos con deduplicación automática")
    logger.info("   Las concesiones duplicadas entre fuentes serán ignoradas")
    logger.info("   gracias a la constraint UNIQUE en la base de datos.")


if __name__ == "__main__":
    asyncio.run(main())
