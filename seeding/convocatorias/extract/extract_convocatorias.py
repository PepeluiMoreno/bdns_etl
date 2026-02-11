# seeding/convocatorias/extract/extract_convocatorias.py


import json
import time
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

RUTA_RAW = Path(__file__).resolve().parent.parent.parent / "data" / "json" / "convocatorias" / "raw"
RUTA_RAW.mkdir(parents=True, exist_ok=True)

# Rate limiting: máximo ~5 req/s compartido entre workers
_REQUEST_DELAY = 0.2  # segundos entre peticiones por worker
_MAX_RETRIES = 5
_BACKOFF_BASE = 2  # segundos base para backoff exponencial


def fetch_convocatoria(codigo: str):
    """Descarga el detalle de una convocatoria por su código BDNS.

    La API devuelve el objeto convocatoria directamente (no envuelto en lista).
    Retorna una lista con un solo elemento para mantener compatibilidad con
    el patrón resultados.extend(result) del caller.
    """
    url = f"https://www.infosubvenciones.es/bdnstrans/api/convocatorias?numConv={codigo}"
    for attempt in range(_MAX_RETRIES):
        try:
            r = requests.get(url, timeout=60)
            if r.status_code == 429:
                wait = _BACKOFF_BASE * (2 ** attempt)
                time.sleep(wait)
                continue
            r.raise_for_status()
            time.sleep(_REQUEST_DELAY)
            data = r.json()
            return [data] if isinstance(data, dict) else data
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 429:
                wait = _BACKOFF_BASE * (2 ** attempt)
                time.sleep(wait)
                continue
            raise
    # Último intento sin capturar
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    data = r.json()
    return [data] if isinstance(data, dict) else data

def main(year: int, mes: int, tipo: str, workers: int):
    # aquí normalmente sacarías los códigos desde la BD
    # placeholder mínimo:
    codigos = []  # TODO: query real

    if not codigos:
        return

    resultados = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(fetch_convocatoria, c) for c in codigos]
        for f in as_completed(futures):
            resultados.extend(f.result())

    if resultados:
        out = RUTA_RAW / f"raw_convocatorias_{tipo}_{year}_{mes:02d}.json"
        with open(out, "w", encoding="utf-8") as f:
            json.dump(resultados, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--year", type=int, required=True)
    p.add_argument("--mes", type=int, required=True)
    p.add_argument("--tipo", type=str, required=True)
    p.add_argument("--workers", type=int, default=6)
    args = p.parse_args()
    main(args.year, args.mes, args.tipo, args.workers)
