"""
Extractor de grandes beneficiarios desde la API BDNS.

Los grandes beneficiarios son personas o entidades que han superado un umbral
de ayuda equivalente en un ejercicio (típicamente 100,000 euros).

Endpoint API: /api/grandesbeneficiarios/busqueda
"""

import json
import requests
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional


class GrandesBeneficiariosExtractor:
    """Extractor de grandes beneficiarios desde BDNS."""

    def __init__(self, output_dir: Optional[Path] = None):
        self.base_url = "https://www.infosubvenciones.es/bdnstrans/api"
        self.output_dir = output_dir or Path(__file__).parent / "data"
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def extract_grandes_beneficiarios_by_year(
        self,
        year: int,
        page_size: int = 5000,
        timeout: int = 180
    ) -> List[Dict[str, Any]]:
        """
        Extrae grandes beneficiarios para un año específico.

        Args:
            year: Año a extraer
            page_size: Registros por página (máximo 5000)
            timeout: Timeout en segundos

        Returns:
            Lista de grandes beneficiarios
        """
        print(f"[GrandesBeneficiarios] Extrayendo año {year}...")

        # Endpoint para búsqueda de grandes beneficiarios
        endpoint = f"{self.base_url}/grandesbeneficiarios/busqueda"

        all_records = []
        page = 0

        while True:
            params = {
                "fechaDesde": f"01/01/{year}",
                "fechaHasta": f"31/12/{year}",
                "page": page,
                "pageSize": page_size
            }

            try:
                response = requests.get(
                    endpoint,
                    params=params,
                    timeout=timeout,
                    headers={"accept": "application/json"}
                )
                response.raise_for_status()
                data = response.json()

                content = data.get("content", [])
                print(f"[GrandesBeneficiarios] Página {page}: {len(content)} registros")

                if not content:
                    break

                all_records.extend(content)

                # Verificar si hay más páginas
                total_elements = data.get("totalElements", 0)
                if len(all_records) >= total_elements:
                    break

                page += 1

            except requests.exceptions.RequestException as e:
                print(f"[GrandesBeneficiarios] Error en página {page}: {e}")
                if page == 0:
                    raise
                break

        print(f"[GrandesBeneficiarios] Total extraído: {len(all_records)} registros")
        return all_records

    def save_to_json(self, data: List[Dict[str, Any]], year: int):
        """Guarda los datos en formato JSON."""
        filename = self.output_dir / f"grandes_beneficiarios_{year}.json"
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"[GrandesBeneficiarios] Guardado en: {filename}")

    def extract_all_years(self, year_from: int, year_to: int):
        """
        Extrae grandes beneficiarios para un rango de años.

        Args:
            year_from: Año inicial
            year_to: Año final (inclusive)
        """
        for year in range(year_from, year_to + 1):
            try:
                data = self.extract_grandes_beneficiarios_by_year(year)
                if data:
                    self.save_to_json(data, year)
            except Exception as e:
                print(f"[GrandesBeneficiarios] Error extrayendo año {year}: {e}")


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Extrae grandes beneficiarios desde la API BDNS"
    )
    parser.add_argument(
        "--year",
        type=int,
        required=True,
        help="Año a extraer"
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        help="Directorio de salida"
    )

    args = parser.parse_args()

    output_dir = Path(args.output_dir) if args.output_dir else None
    extractor = GrandesBeneficiariosExtractor(output_dir=output_dir)

    data = extractor.extract_grandes_beneficiarios_by_year(args.year)
    extractor.save_to_json(data, args.year)


if __name__ == "__main__":
    main()
