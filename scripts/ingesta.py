"""Baja quincenas de Profeco y deja en el repo de datos los dos cortes de la decisión #2.

Corre en GitHub Actions, no en Fabric. El backfill y el cron son la misma corrida: el
manifiesto dice qué falta y `--max` limita cuántas se procesan por tanda.

De cada CSV (~155 MB) sobreviven sólo las filas del catálogo de `objetivo.yml` y las
tuplas distintas de tienda, estas del archivo completo. El original no se guarda; el
`sha256` del manifiesto es lo que permite rehacer cualquier corte desde la fuente.

Ver docs/decisiones.md.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import tempfile
import urllib.error
import urllib.request
from datetime import UTC, date, datetime
from pathlib import Path
from typing import NamedTuple

import polars as pl
import yaml

BASE = "https://repodatos.atdt.gob.mx/api_update/profeco"

# La fuente no publica nada anterior: 01-2023 responde igual que el futuro.
PRIMERA = (2024, 1, 1)

# Lo que se guarda de cada tienda. La llave es (latitud, longitud, nombre_comercial);
# el resto viaja para que silver arme dim_tienda sin volver a la fuente.
COLUMNAS_TIENDA = [
    "nombre_comercial",
    "cadena_comercial",
    "giro",
    "direccion",
    "estado",
    "municipio",
    "latitud",
    "longitud",
]

RAIZ = Path(__file__).resolve().parent.parent


class Quincena(NamedTuple):
    anio: int
    mes: int
    q: int

    @property
    def etiqueta(self) -> str:
        return f"{self.anio}-{self.mes:02d}_q{self.q}"

    @property
    def url(self) -> str:
        return (
            f"{BASE}/programa_quien_es_quien_precios_{self.anio}"
            f"/{self.mes:02d}-{self.anio}_{self.q:02d}.csv"
        )


def quincenas(hasta: date) -> list[Quincena]:
    """Todas las quincenas entre PRIMERA y el mes de `hasta`, publicadas o no."""
    return [
        Quincena(anio, mes, q)
        for anio in range(PRIMERA[0], hasta.year + 1)
        for mes in range(1, 13)
        for q in (1, 2)
        if (anio, mes, q) >= PRIMERA and (anio, mes) <= (hasta.year, hasta.month)
    ]


def leer_manifiesto(ruta: Path) -> list[dict]:
    if not ruta.exists():
        return []
    return [
        json.loads(linea)
        for linea in ruta.read_text(encoding="utf-8").splitlines()
        if linea.strip()
    ]


def pendientes(todas: list[Quincena], manifiesto: list[dict]) -> list[Quincena]:
    """Las que aún no tienen entrada en el manifiesto.

    Que Profeco republique una quincena corregida no se detecta aquí: exigiría rebajar
    los 7 GB del histórico en cada corrida. Para eso está `--rehacer`, que la vuelve a
    bajar con un `intento` nuevo y conserva las dos versiones.
    """
    hechas = {e["quincena"] for e in manifiesto}
    return [q for q in todas if q.etiqueta not in hechas]


# El CDN de la fuente contesta 403 si la petición no trae `Accept`. urllib no lo manda
# por su cuenta y curl sí, que es por qué lo mismo funciona en la terminal y aquí no.
CABECERAS = {
    "Accept": "*/*",
    "User-Agent": "indice-gansito (+https://github.com/AldoMor00/indice-gansito)",
}

# Lo que la fuente devuelve por una quincena que no existe. Responde 503, no 404, y no
# hay forma de distinguirlo de una caída real; el 404 va por si algún día lo arreglan.
NO_PUBLICADA = (404, 503)


def descarga(url: str, destino: Path) -> tuple[str, int] | None:
    """Baja `url` a `destino`. Devuelve (sha256, bytes), o None si no está publicada.

    Cualquier otro error se propaga: un 403 leído como "todavía no sale" dejaría al
    pipeline sin bajar nada y sin quejarse. La tanda es reanudable, así que tronar es
    barato —el manifiesto ya tiene lo que alcanzó a escribir.
    """
    digest = hashlib.sha256()
    total = 0
    peticion = urllib.request.Request(url, headers=CABECERAS)
    try:
        with (
            urllib.request.urlopen(peticion, timeout=180) as respuesta,
            destino.open("wb") as f,
        ):
            while trozo := respuesta.read(1 << 20):
                digest.update(trozo)
                f.write(trozo)
                total += len(trozo)
    except urllib.error.HTTPError as e:
        if e.code in NO_PUBLICADA:
            return None
        raise
    return digest.hexdigest(), total


def lee_csv(ruta: Path) -> pl.DataFrame:
    """Todo como texto: bronze no castea. `utf8-lossy` absorbe el BOM."""
    return pl.read_csv(ruta, encoding="utf8-lossy", infer_schema_length=0)


def corte_precios(df: pl.DataFrame, productos: list[str]) -> pl.DataFrame:
    """Las filas del catálogo objetivo.

    No se filtra por `catalogo`: el mismo SKU aparece en `Basicos` y en `Mercados`.
    """
    return df.filter(pl.col("producto").is_in(productos)).sort(df.columns)


def corte_tiendas(df: pl.DataFrame) -> pl.DataFrame:
    """Tiendas distintas del archivo **completo**, no del corte de precios.

    Si salieran del corte, dim_tienda quedaría sesgada a las que venden pastelillos.
    """
    return df.select(COLUMNAS_TIENDA).unique().sort(COLUMNAS_TIENDA)


def rutas(destino: Path, q: Quincena, intento: int) -> tuple[Path, Path]:
    """Un `intento` mayor que 1 lleva sufijo para no pisar la versión anterior."""
    sufijo = "" if intento == 1 else f"_i{intento}"
    nombre = f"{q.anio}-{q.mes:02d}_q{q.q}{sufijo}.parquet"
    return (
        destino / "precios" / f"anio={q.anio}" / f"qqp_{nombre}",
        destino / "tiendas" / f"anio={q.anio}" / f"tiendas_{nombre}",
    )


def escribe(df: pl.DataFrame, ruta: Path) -> None:
    ruta.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(ruta, compression="zstd")


def procesa(q: Quincena, productos: list[str], destino: Path, intento: int) -> dict | None:
    """Baja, corta y escribe una quincena. Devuelve su entrada de manifiesto, o None."""
    with tempfile.TemporaryDirectory() as tmp:
        crudo = Path(tmp) / "crudo.csv"
        bajado = descarga(q.url, crudo)
        if bajado is None:
            print(f"  {q.etiqueta}: no publicada")
            return None
        sha, total = bajado

        df = lee_csv(crudo)
        precios = corte_precios(df, productos)
        tiendas = corte_tiendas(df)

    ruta_precios, ruta_tiendas = rutas(destino, q, intento)
    escribe(precios, ruta_precios)
    escribe(tiendas, ruta_tiendas)

    print(
        f"  {q.etiqueta}: {df.height:,} filas leídas, {precios.height:,} al corte, "
        f"{tiendas.height:,} tiendas"
    )
    return {
        "url_origen": q.url,
        "sha256": sha,
        "bytes": total,
        "filas_leidas": df.height,
        "filas_filtradas": precios.height,
        "quincena": q.etiqueta,
        "descargado_utc": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "intento": intento,
    }


def main() -> int:
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("--destino", type=Path, required=True, help="clon de indice-gansito-datos")
    cli.add_argument("--max", type=int, default=5, help="cuántas quincenas por tanda")
    cli.add_argument("--objetivo", type=Path, default=RAIZ / "objetivo.yml")
    cli.add_argument(
        "--rehacer",
        action="append",
        default=[],
        metavar="ETIQUETA",
        help="vuelve a bajar una quincena ya hecha, p. ej. 2025-11_q2",
    )
    args = cli.parse_args()

    productos = yaml.safe_load(args.objetivo.read_text(encoding="utf-8"))["producto"]
    manifiesto_ruta = args.destino / "manifiesto.jsonl"
    manifiesto = leer_manifiesto(manifiesto_ruta)

    todas = quincenas(date.today())
    falta = pendientes(todas, manifiesto)

    if args.rehacer:
        por_etiqueta = {q.etiqueta: q for q in todas}
        desconocidas = [e for e in args.rehacer if e not in por_etiqueta]
        if desconocidas:
            print(f"quincenas desconocidas: {desconocidas}", file=sys.stderr)
            return 1
        cola, tope = [por_etiqueta[e] for e in args.rehacer], len(args.rehacer)
    else:
        cola, tope = falta, args.max

    print(f"catálogo  : {productos}")
    print(f"destino   : {args.destino}")
    print(f"pendientes: {len(falta)}, tope de la tanda: {tope}")

    intentos = {}
    for entrada in manifiesto:
        intentos[entrada["quincena"]] = max(
            intentos.get(entrada["quincena"], 0), entrada["intento"]
        )

    # El tope cuenta quincenas procesadas, no intentadas: las no publicadas son una
    # petición que muere en el 503, y saltárselas es lo que evita que un hueco
    # permanente en la fuente deje al cron atorado antes de llegar a lo que sí salió.
    hechas = []
    with manifiesto_ruta.open("a", encoding="utf-8") as f:
        for q in cola:
            if len(hechas) >= tope:
                break
            entrada = procesa(q, productos, args.destino, intentos.get(q.etiqueta, 0) + 1)
            if entrada is None:
                continue
            f.write(json.dumps(entrada, ensure_ascii=False) + "\n")
            f.flush()
            hechas.append(q.etiqueta)

    print(f"procesadas: {len(hechas)} {hechas}")
    if salida := os.environ.get("GITHUB_OUTPUT"):
        with Path(salida).open("a", encoding="utf-8") as f:
            f.write(f"procesadas={' '.join(hechas)}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
