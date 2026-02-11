#!/usr/bin/env python3
"""
Transforma JSONs de concesiones a CSVs para carga rápida con COPY.

Procesa:
1. Beneficiarios → beneficiarios.csv
2. Convocatorias → convocatorias.csv
3. Concesiones → concesiones.csv

Los CSVs generados están listos para COPY FROM con formato:
- Delimiter: |
- Quote: "
- NULL: \\N
- Escape: \\

Uso:
    python transform_json_to_csv.py /path/to/data.json partidos_politicos /output/dir/
"""
import json
import csv
import sys
import re
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Set, Tuple
from uuid import uuid4


def normalizar(texto: str) -> str:
    """Normaliza texto para búsqueda."""
    if not texto:
        return ""

    # Minúsculas
    texto = texto.lower()

    # Eliminar acentos
    replacements = {
        'á': 'a', 'é': 'e', 'í': 'i', 'ó': 'o', 'ú': 'u', 'ü': 'u',
        'à': 'a', 'è': 'e', 'ì': 'i', 'ò': 'o', 'ù': 'u',
        'ä': 'a', 'ë': 'e', 'ï': 'i', 'ö': 'o', 'ü': 'u',
        'ñ': 'n', 'ç': 'c'
    }
    for old, new in replacements.items():
        texto = texto.replace(old, new)

    # Eliminar caracteres especiales excepto letras, números, espacios
    texto = re.sub(r'[^a-z0-9\s]', ' ', texto)

    # Normalizar espacios
    texto = ' '.join(texto.split())

    return texto


def extract_nif(beneficiario_str: str) -> str:
    """Extrae NIF/CIF del string de beneficiario."""
    if not beneficiario_str:
        return ""

    # Formato típico: "Q2826000J - CONSEJERIA DE ECONOMIA..."
    parts = beneficiario_str.split(' - ', 1)
    if len(parts) >= 1:
        nif = parts[0].strip().upper()
        # Limpiar sufijos como :OT, :LO, etc
        nif = re.sub(r':[A-Z]{2}$', '', nif)
        return nif

    return ""


def extract_nombre(beneficiario_str: str) -> str:
    """Extrae nombre del beneficiario del string."""
    if not beneficiario_str:
        return ""

    parts = beneficiario_str.split(' - ', 1)
    if len(parts) >= 2:
        return parts[1].strip()

    # Si no hay separador, usar todo el string
    return beneficiario_str.strip()


def parse_date(date_str: str) -> str:
    """Convierte fecha de API a formato PostgreSQL."""
    if not date_str:
        return "\\N"

    try:
        # Formato API: "31/05/2024" o "2024-05-31"
        if '/' in date_str:
            dt = datetime.strptime(date_str, '%d/%m/%Y')
        else:
            dt = datetime.strptime(date_str, '%Y-%m-%d')

        return dt.strftime('%Y-%m-%d')
    except:
        return "\\N"


def safe_float(val) -> str:
    """Convierte a float o NULL."""
    if val is None or val == '':
        return "\\N"
    try:
        return str(float(val))
    except:
        return "\\N"


def safe_str(val) -> str:
    """Escapa string para CSV o NULL."""
    if val is None or val == '':
        return "\\N"

    # Convertir a string y limpiar
    s = str(val).strip()
    if not s:
        return "\\N"

    # Escapar caracteres especiales
    s = s.replace('\\', '\\\\')  # Backslash
    s = s.replace('\n', ' ')     # Newlines
    s = s.replace('\r', ' ')     # Carriage returns
    s = s.replace('\t', ' ')     # Tabs
    s = s.replace('|', ' ')      # Delimiter

    return s


class JSONToCSVTransformer:
    """Transforma JSON de concesiones a CSVs optimizados para COPY."""

    def __init__(self, json_path: Path, regimen_tipo: str, output_dir: Path):
        self.json_path = json_path
        self.regimen_tipo = regimen_tipo
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Tablas temporales para deduplicación
        self.beneficiarios: Dict[str, Dict] = {}  # nif → data
        self.convocatorias: Dict[str, Dict] = {}  # codigo_bdns → data
        self.concesiones: List[Dict] = []

    def load_json(self) -> List[Dict]:
        """Carga JSON de concesiones."""
        print(f"Cargando JSON: {self.json_path}")
        with open(self.json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        print(f"  ✓ {len(data)} registros cargados")
        return data

    def process_beneficiario(self, beneficiario_str: str) -> str:
        """Procesa beneficiario y retorna UUID."""
        if not beneficiario_str:
            return None

        nif = extract_nif(beneficiario_str)
        if not nif:
            return None

        # Ya existe?
        if nif in self.beneficiarios:
            return self.beneficiarios[nif]['id']

        # Crear nuevo
        nombre = extract_nombre(beneficiario_str)
        nombre_norm = normalizar(nombre)

        benef_id = str(uuid4())
        self.beneficiarios[nif] = {
            'id': benef_id,
            'nif': nif,
            'nombre': nombre[:500],  # Límite de columna
            'nombre_norm': nombre_norm[:500],
            'created_by': 'etl_transform',
            'created_at': datetime.now().isoformat()
        }

        return benef_id

    def process_convocatoria(self, record: Dict) -> str:
        """Procesa convocatoria y retorna UUID."""
        # codigoBDNS es el identificador natural
        codigo_bdns = record.get('codigoBDNS', record.get('codigo_bdns'))
        if not codigo_bdns:
            # Fallback: usar ID de convocatoria si existe
            codigo_bdns = record.get('convocatoria', {}).get('id') if isinstance(record.get('convocatoria'), dict) else record.get('convocatoria')

        if not codigo_bdns:
            return None

        # Ya existe?
        if codigo_bdns in self.convocatorias:
            return self.convocatorias[codigo_bdns]['id']

        # Crear nueva
        conv_id = str(uuid4())

        # Título de convocatoria
        titulo = None
        if isinstance(record.get('convocatoria'), dict):
            titulo = record.get('convocatoria', {}).get('titulo')

        self.convocatorias[codigo_bdns] = {
            'id': conv_id,
            'id_bdns': codigo_bdns,  # ID natural de BDNS
            'codigo_bdns': codigo_bdns,  # Mismo valor por compatibilidad
            'titulo': titulo[:500] if titulo else None,
            'created_by': 'etl_transform',
            'created_at': datetime.now().isoformat()
        }

        return conv_id

    def process_concesion(self, record: Dict) -> Dict:
        """Procesa concesión y retorna registro para CSV."""
        # IDs foráneos
        beneficiario_id = self.process_beneficiario(record.get('beneficiario'))
        convocatoria_id = self.process_convocatoria(record)

        if not beneficiario_id or not convocatoria_id:
            return None

        # ID de concesión (natural de BDNS)
        id_concesion = record.get('id')
        if not id_concesion:
            return None

        # Fecha de concesión
        fecha_concesion = parse_date(record.get('fechaConcesion', record.get('fecha_concesion')))
        if fecha_concesion == "\\N":
            return None

        # Importes
        importe_nominal = safe_float(record.get('importeConcedidoNominal', record.get('importe_nominal')))
        importe_equivalente = safe_float(record.get('importeConcedido', record.get('importe_equivalente')))

        return {
            'id': str(uuid4()),  # UUID generado
            'id_concesion': id_concesion,
            'beneficiario_id': beneficiario_id,
            'convocatoria_id': convocatoria_id,
            'fecha_concesion': fecha_concesion,
            'regimen_tipo': self.regimen_tipo,
            'importe_nominal': importe_nominal,
            'importe_equivalente': importe_equivalente,
            'created_by': 'etl_transform',
            'created_at': datetime.now().isoformat()
        }

    def transform(self):
        """Ejecuta transformación completa."""
        # Cargar JSON
        records = self.load_json()

        # Procesar cada registro
        print(f"\nProcesando {len(records)} registros...")
        for i, record in enumerate(records, 1):
            concesion = self.process_concesion(record)
            if concesion:
                self.concesiones.append(concesion)

            if i % 1000 == 0:
                print(f"  Procesados: {i}/{len(records)} ({i*100//len(records)}%)")

        print(f"\n✓ Transformación completada:")
        print(f"  Beneficiarios: {len(self.beneficiarios)}")
        print(f"  Convocatorias: {len(self.convocatorias)}")
        print(f"  Concesiones:   {len(self.concesiones)}")

    def write_csvs(self):
        """Escribe CSVs optimizados para COPY."""
        # CSV de beneficiarios
        benef_csv = self.output_dir / 'beneficiarios.csv'
        print(f"\nEscribiendo {benef_csv}...")
        with open(benef_csv, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f, delimiter='|', quoting=csv.QUOTE_MINIMAL, escapechar='\\')
            # Header
            writer.writerow(['id', 'nif', 'nombre', 'nombre_norm', 'created_by', 'created_at'])
            # Data
            for b in self.beneficiarios.values():
                writer.writerow([
                    b['id'],
                    safe_str(b['nif']),
                    safe_str(b['nombre']),
                    safe_str(b['nombre_norm']),
                    b['created_by'],
                    b['created_at']
                ])
        print(f"  ✓ {len(self.beneficiarios)} beneficiarios escritos")

        # CSV de convocatorias
        conv_csv = self.output_dir / 'convocatorias.csv'
        print(f"\nEscribiendo {conv_csv}...")
        with open(conv_csv, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f, delimiter='|', quoting=csv.QUOTE_MINIMAL, escapechar='\\')
            # Header
            writer.writerow(['id', 'id_bdns', 'codigo_bdns', 'titulo', 'created_by', 'created_at'])
            # Data
            for c in self.convocatorias.values():
                writer.writerow([
                    c['id'],
                    safe_str(c['id_bdns']),
                    safe_str(c['codigo_bdns']),
                    safe_str(c['titulo']),
                    c['created_by'],
                    c['created_at']
                ])
        print(f"  ✓ {len(self.convocatorias)} convocatorias escritas")

        # CSV de concesiones
        conc_csv = self.output_dir / 'concesiones.csv'
        print(f"\nEscribiendo {conc_csv}...")
        with open(conc_csv, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f, delimiter='|', quoting=csv.QUOTE_MINIMAL, escapechar='\\')
            # Header
            writer.writerow([
                'id', 'id_concesion', 'beneficiario_id', 'convocatoria_id',
                'fecha_concesion', 'regimen_tipo', 'importe_nominal', 'importe_equivalente',
                'created_by', 'created_at'
            ])
            # Data
            for c in self.concesiones:
                writer.writerow([
                    c['id'],
                    safe_str(c['id_concesion']),
                    c['beneficiario_id'],
                    c['convocatoria_id'],
                    c['fecha_concesion'],
                    c['regimen_tipo'],
                    c['importe_nominal'],
                    c['importe_equivalente'],
                    c['created_by'],
                    c['created_at']
                ])
        print(f"  ✓ {len(self.concesiones)} concesiones escritas")


def main():
    if len(sys.argv) < 4:
        print("Uso: python transform_json_to_csv.py <json_path> <regimen_tipo> <output_dir>")
        print()
        print("Ejemplo:")
        print("  python transform_json_to_csv.py data.json partidos_politicos ./output/")
        print()
        print("regimen_tipo valores:")
        print("  - minimis")
        print("  - ayuda_estado")
        print("  - partidos_politicos")
        print("  - grandes_benef")
        print("  - ordinaria")
        sys.exit(1)

    json_path = Path(sys.argv[1])
    regimen_tipo = sys.argv[2]
    output_dir = Path(sys.argv[3])

    if not json_path.exists():
        print(f"Error: JSON no encontrado: {json_path}")
        sys.exit(1)

    print(f"{'='*60}")
    print(f"TRANSFORM JSON → CSV")
    print(f"{'='*60}")
    print(f"JSON:         {json_path}")
    print(f"Régimen:      {regimen_tipo}")
    print(f"Output:       {output_dir}")
    print()

    # Transformar
    transformer = JSONToCSVTransformer(json_path, regimen_tipo, output_dir)
    transformer.transform()
    transformer.write_csvs()

    print(f"\n{'='*60}")
    print("✓ Transformación completada")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
