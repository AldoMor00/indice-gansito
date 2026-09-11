"""Baja el INPC quincenal de INEGI y lo deja entero en el repo de datos.

Corre en GitHub Actions, no en Fabric. Se parece a la ingesta de CONASAMI y no a la de
Profeco: son 140 KB, así que la decisión #2 no aplica y se guarda el JSON tal como lo
sirve la API. Tampoco hay período que enumerar —es la misma serie, revisada—, así que lo
que dice si hay algo que hacer es el `sha256`: se baja siempre y se escribe sólo si
cambió. Cuando cambia entra una versión nueva con sufijo `_vN` y las anteriores se quedan.

El token va en la URL, así que **no entra al repo**: el manifiesto guarda la URL con el
hueco donde iba. Ver docs/decisiones.md.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import tempfile
from datetime import UTC, datetime
from pathlib import Path

from fuente import descarga, leer_manifiesto, salida_actions

BASE = "https://www.inegi.org.mx/app/api/indicadores/desarrolladores/jsonxml/INDICATOR"

# INPC general, **nivel del índice**, base 2018=100 y grano quincenal. El id no está en la
# documentación de INEGI: salió de recorrer el catálogo de su navegador de indicadores. Se
# reconoce por tema 189128 ("Índice"), unidad 1051 ("Índice base 2018=100") y frecuencia 15
# ("Quincenal"), que es lo que permitiría volver a encontrarlo si algún día se mueve. Ojo
# con los vecinos: casi todos los ids de alrededor son variación porcentual, no nivel.
INDICADOR = "910420"

# `BIE-BISE`, no `BIE`. Con `BIE` la API contesta 400 y "No se encontraron resultados",
# que es exactamente el mismo error que da un token inválido: no se distinguen.
FUENTE = "BIE-BISE"

# Geografía nacional. `0700` —que aparece en ejemplos viejos— da el mismo 400 mudo.
GEO = "00"

# Lo que se escribe en `url_origen` en lugar del token.
MASCARA = "{token}"

# Todo lo que escribe esta fuente vive bajo este directorio del repo de datos.
ZONA = "inpc"

ARCHIVO = "inpc_quincenal"


def url_de(token: str) -> str:
    return f"{BASE}/{INDICADOR}/es/{GEO}/false/{FUENTE}/2.0/{token}?type=json"


def ruta(destino: Path, version: int) -> Path:
    """Una versión mayor que 1 lleva sufijo para no pisar la anterior."""
    sufijo = "" if version == 1 else f"_v{version}"
    return destino / ZONA / "serie" / f"{ARCHIVO}{sufijo}.json"


def ultima(manifiesto: list[dict]) -> dict | None:
    """La entrada de mayor versión, o None si nunca se ha bajado."""
    return max(manifiesto, key=lambda e: e["version"], default=None)


def observaciones(cruda: Path) -> tuple[int, str, str]:
    """(cuántas, último periodo, LASTUPDATE) del JSON que sirvió la API.

    Parsear de verdad es lo que caza una respuesta truncada o un cuerpo de error que
    llegó con 200. La API contesta 400 cuando el token o el id están mal, así que ese
    caso ya lo detuvo `descarga`; esto cubre el resto.
    """
    datos = json.loads(cruda.read_text(encoding="utf-8"))
    series = datos.get("Series") if isinstance(datos, dict) else None
    if not series or not series[0].get("OBSERVATIONS"):
        raise RuntimeError(f"la API no devolvió observaciones: {str(datos)[:200]}")
    serie = series[0]
    obs = serie["OBSERVATIONS"]
    return len(obs), max(o["TIME_PERIOD"] for o in obs), serie.get("LASTUPDATE", "")


def procesa(token: str, destino: Path, manifiesto: list[dict]) -> dict | None:
    """Baja la serie y la escribe si cambió. Devuelve su entrada de manifiesto, o None.

    None es el estado estable: la serie sigue siendo la misma que ya está en el repo.
    """
    previa = ultima(manifiesto)

    with tempfile.TemporaryDirectory() as tmp:
        cruda = Path(tmp) / f"{ARCHIVO}.json"
        bajado = descarga(url_de(token), cruda)
        if bajado is None:
            print(f"  {ARCHIVO}: no publicado")
            return None
        sha, total = bajado

        if previa and previa["sha256"] == sha:
            print(f"  {ARCHIVO}: sin cambios (v{previa['version']})")
            return None

        n, ultimo, actualizado = observaciones(cruda)
        version = previa["version"] + 1 if previa else 1
        salida = ruta(destino, version)
        salida.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(cruda, salida)

    print(f"  {ARCHIVO}: v{version}, {n:,} observaciones hasta {ultimo}, {total:,} bytes")
    return {
        "url_origen": url_de(MASCARA),
        "sha256": sha,
        "bytes": total,
        "observaciones": n,
        "ultimo_periodo": ultimo,
        "actualizado_fuente": actualizado,
        "archivo": ARCHIVO,
        "descargado_utc": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "version": version,
    }


def main() -> int:
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("--destino", type=Path, required=True, help="clon de indice-gansito-datos")
    args = cli.parse_args()

    token = os.environ.get("INEGI_TOKEN", "").strip()
    if not token:
        print("falta INEGI_TOKEN en el entorno", file=sys.stderr)
        return 1

    manifiesto_ruta = args.destino / ZONA / "manifiesto.jsonl"
    manifiesto = leer_manifiesto(manifiesto_ruta)

    print(f"destino    : {args.destino}")
    print(f"indicador  : {INDICADOR} ({FUENTE}), manifiesto con {len(manifiesto)} entradas")

    entrada = procesa(token, args.destino, manifiesto)
    if entrada is not None:
        manifiesto_ruta.parent.mkdir(parents=True, exist_ok=True)
        with manifiesto_ruta.open("a", encoding="utf-8") as f:
            f.write(json.dumps(entrada, ensure_ascii=False) + "\n")

    hecho = f"{ARCHIVO} v{entrada['version']}" if entrada else ""
    print(f"actualizados: {1 if entrada else 0} {[hecho] if hecho else []}")
    salida_actions(actualizados=hecho)
    return 0


if __name__ == "__main__":
    sys.exit(main())
