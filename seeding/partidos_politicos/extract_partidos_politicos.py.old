"""
Extractor de ayudas a partidos políticos desde la API BDNS.

Este endpoint contiene concesiones y beneficiarios que no aparecen en el endpoint
regular de concesiones, por lo que es necesario extraerlos por separado.

Endpoint API: /api/partidospoliticos/busqueda
"""

import json
import requests
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional


class PartidosPoliticosExtractor:
    """Extractor de ayudas a partidos políticos desde BDNS."""

    def __init__(self, output_dir: Optional[Path] = None):
        self.base_url = "https://www.infosubvenciones.es/bdnstrans/api"
        self.output_dir = output_dir or Path(__file__).parent / "data"
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def extract_partidos_by_year(
        self,
        year: int,
        page_size: int = 10000,
        timeout: int = 180
    ) -> List[Dict[str, Any]]:
        """
        Extrae ayudas a partidos políticos para un año específico.

        Args:
            year: Año a extraer
            page_size: Registros por página
            timeout: Timeout en segundos

        Returns:
            Lista de ayudas a partidos políticos
        """
        print(f"[PartidosPoliticos] Extrayendo año {year}...")

        endpoint = f"{self.base_url}/partidospoliticos/busqueda"

        all_records = []
        page = 0

        while True:
            params = {
                "page": page,
                "pageSize": page_size,
                "order": "numeroConvocatoria",
                "direccion": "asc",
                "vpd": "GE",
                "descripcion": "Resolución",
                "descripcionTipoBusqueda": "0",
                "numeroConvocatoria": " ",
                "codConcesion": " ",
                "fechaDesde": f"01/01/{year}",
                "fechaHasta": f"31/12/{year}",
                "nifCif": " "
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
                print(f"[PartidosPoliticos] Página {page}: {len(content)} registros")

                if not content:
                    break

                all_records.extend(content)

                # Verificar si hay más páginas
                total_elements = data.get("totalElements", 0)
                if len(all_records) >= total_elements:
                    break

                page += 1

            except requests.exceptions.RequestException as e:
                print(f"[PartidosPoliticos] Error en página {page}: {e}")
                if page == 0:
                    raise
                break

        print(f"[PartidosPoliticos] Total extraído: {len(all_records)} registros")
        return all_records

    def extract_partido_detalle(self, codigo: str, timeout: int = 60) -> Optional[Dict[str, Any]]:
        """
        Extrae el detalle completo de una ayuda a partido político.

        Args:
            codigo: Código de la concesión
            timeout: Timeout en segundos

        Returns:
            Detalle de la ayuda o None si hay error
        """
        endpoint = f"{self.base_url}/partidospoliticos/{codigo}"

        try:
            response = requests.get(endpoint, timeout=timeout)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"[PartidosPoliticos] Error obteniendo detalle {codigo}: {e}")
            return None

    def save_to_json(self, data: List[Dict[str, Any]], year: int):
        """Guarda los datos en formato JSON."""
        filename = self.output_dir / f"partidos_politicos_{year}.json"
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"[PartidosPoliticos] Guardado en: {filename}")

    def extract_all_years(self, year_from: int, year_to: int):
        """
        Extrae partidos políticos para un rango de años.

        Args:
            year_from: Año inicial
            year_to: Año final (inclusive)
        """
        for year in range(year_from, year_to + 1):
            try:
                data = self.extract_partidos_by_year(year)
                if data:
                    self.save_to_json(data, year)
            except Exception as e:
                print(f"[PartidosPoliticos] Error extrayendo año {year}: {e}")


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Extrae ayudas a partidos políticos desde la API BDNS"
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
    extractor = PartidosPoliticosExtractor(output_dir=output_dir)

    data = extractor.extract_partidos_by_year(args.year)
    extractor.save_to_json(data, args.year)


if __name__ == "__main__":
    main()
