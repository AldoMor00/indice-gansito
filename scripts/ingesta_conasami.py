"""Baja los CSV de salario mínimo de CONASAMI y los deja enteros en el repo de datos.

Corre en GitHub Actions, no en Fabric. No se parece a la ingesta de Profeco y no debe:

- **No se corta.** Los dos archivos suman ~40 KB, así que la decisión #2 no aplica y lo
  que se guarda son los bytes que sirvió el host, tal cual. Eso es lo que hace que el
  `sha256` del manifiesto signifique algo aquí.
- **No hay período.** La unidad no es "un lote nuevo cada quincena" sino "el mismo
  archivo, revisado". Lo que dice si hay algo que hacer es el hash, no una etiqueta:
  se baja siempre y se escribe sólo si cambió. Profeco no puede hacer eso —serían 7 GB
  por corrida—; aquí cuesta 40 KB.

Cuando el hash cambia entra una versión nueva con sufijo `_vN`, igual que el `_iN` de
Profeco, y las anteriores se quedan. Ver docs/decisiones.md.
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
import tempfile
from datetime import UTC, datetime
from pathlib import Path

import polars as pl
from fuente import descarga, leer_manifiesto, salida_actions

BASE = "https://repodatos.atdt.gob.mx/api_update/conasami/salarios_minimos"

# Todo lo que escribe esta fuente vive bajo este directorio del repo de datos.
ZONA = "conasami"

# Los dos que responden preguntas del proyecto. `sm_real_indice` trae el nominal mensual
# y el deflactor (`smg_nominal / smg_real` es el INPC entre 100); `..._zonas` trae el
# salario vigente por zona. Los otros dos del catálogo —el histórico anual y el de la
# capital— no responden ninguna, y el anual además discrepa del mensual por centavos.
ARCHIVOS = ("sm_real_indice", "sm_general_profesionales_zonas")


def url_de(archivo: str) -> str:
    return f"{BASE}/{archivo}.csv"


def ruta(destino: Path, archivo: str, version: int) -> Path:
    """Una versión mayor que 1 lleva sufijo para no pisar la anterior."""
    sufijo = "" if version == 1 else f"_v{version}"
    return destino / ZONA / "salarios" / f"{archivo}{sufijo}.csv"


def ultima(manifiesto: list[dict], archivo: str) -> dict | None:
    """La entrada de mayor versión de `archivo`, o None si nunca se ha bajado."""
    de_ese = [e for e in manifiesto if e["archivo"] == archivo]
    return max(de_ese, key=lambda e: e["version"], default=None)


def filas(csv: Path) -> int:
    """Cuenta parseando de verdad: un CSV truncado o una página de error del CDN se cae
    aquí, antes de entrar al repo. Todo como texto, que bronze no castea."""
    return pl.read_csv(csv, encoding="utf8-lossy", infer_schema_length=0).height


def procesa(archivo: str, destino: Path, manifiesto: list[dict]) -> dict | None:
    """Baja `archivo` y lo escribe si cambió. Devuelve su entrada de manifiesto, o None.

    None cubre los dos casos en que no hay nada que commitear: que no esté publicado y
    que sea idéntico a lo que ya se tiene. El segundo es el estado estable.
    """
    previa = ultima(manifiesto, archivo)
    url = url_de(archivo)

    with tempfile.TemporaryDirectory() as tmp:
        crudo = Path(tmp) / f"{archivo}.csv"
        bajado = descarga(url, crudo)
        if bajado is None:
            print(f"  {archivo}: no publicado")
            return None
        sha, total = bajado

        if previa and previa["sha256"] == sha:
            print(f"  {archivo}: sin cambios (v{previa['version']})")
            return None

        n = filas(crudo)
        version = previa["version"] + 1 if previa else 1
        salida = ruta(destino, archivo, version)
        salida.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(crudo, salida)

    print(f"  {archivo}: v{version}, {n:,} filas, {total:,} bytes")
    return {
        "url_origen": url,
        "sha256": sha,
        "bytes": total,
        "filas": n,
        "archivo": archivo,
        "descargado_utc": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "version": version,
    }


def main() -> int:
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("--destino", type=Path, required=True, help="clon de indice-gansito-datos")
    args = cli.parse_args()

    manifiesto_ruta = args.destino / ZONA / "manifiesto.jsonl"
    manifiesto = leer_manifiesto(manifiesto_ruta)

    print(f"destino : {args.destino}")
    print(f"archivos: {len(ARCHIVOS)}, manifiesto con {len(manifiesto)} entradas")

    hechas = []
    manifiesto_ruta.parent.mkdir(parents=True, exist_ok=True)
    with manifiesto_ruta.open("a", encoding="utf-8") as f:
        for archivo in ARCHIVOS:
            entrada = procesa(archivo, args.destino, manifiesto)
            if entrada is None:
                continue
            f.write(json.dumps(entrada, ensure_ascii=False) + "\n")
            f.flush()
            hechas.append(f"{archivo} v{entrada['version']}")

    print(f"actualizados: {len(hechas)} {hechas}")
    salida_actions(actualizados=", ".join(hechas))
    return 0


if __name__ == "__main__":
    sys.exit(main())
