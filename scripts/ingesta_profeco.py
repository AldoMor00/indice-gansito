"""Baja quincenas de Profeco y deja en el repo de datos los dos cortes de la decisión #2.

Corre en GitHub Actions, no en Fabric. El backfill y el cron son la misma corrida: el
manifiesto dice qué falta y `--max` limita cuántas se procesan por tanda.

La fuente sirve un bundle por año y no un archivo por quincena (decisión #34): se baja el
del año que tenga pendientes y se saca de él sólo lo que falte. De cada CSV (~155 MB)
sobreviven las filas del catálogo de `objetivo.yml` y las tuplas distintas de tienda,
estas del archivo completo. El original no se guarda; el `sha256` del manifiesto —el del
CSV, nunca el del bundle— es lo que permite rehacer cualquier corte desde la fuente.

Todo lo de esta fuente cuelga de `profeco/` en el repo de datos, manifiesto incluido.
Ver docs/decisiones.md.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import shutil
import sys
import tempfile
import zipfile
from collections.abc import Iterator
from contextlib import closing
from datetime import UTC, date, datetime
from pathlib import Path
from typing import NamedTuple

import polars as pl
import yaml
from fuente import descarga, leer_manifiesto, salida_actions

BASE = "https://datos.profeco.gob.mx/datos_abiertos"
LISTADO = f"{BASE}/qqp.php"

# El token de cada año es una cadena asignada a mano, no derivable de nada: el de 2024
# termina en 95c5 y el del diccionario en 95c7. La única forma de saber el de 2027 es
# leerlo del listado el día que aparezca su `li`, y es lo que hace automático el cambio
# de año. Si un día cambian la plantilla, el mapa sale vacío y la corrida truena.
ENLACE_ANIO = re.compile(
    r'href="(file\.php\?t=[0-9a-f]+)"[^>]*>\s*Quien es Quien en los Precios\s+(\d{4})\s*<',
    re.IGNORECASE,
)
ENLACE_METADATOS = re.compile(
    r'href="(file\.php\?t=[0-9a-f]+)"[^>]*>\s*Metadatos dataset\s*<', re.IGNORECASE
)

# El metadato declara el mes publicado más reciente y pesa 877 bytes: es la sonda que
# evita bajar los 186 MB del bundle —2.5 GB de CSV adentro— para descubrir que no trae
# nada nuevo. Hace falta porque el bundle no expone `Last-Modified`, ni `ETag`, ni
# `Content-Length`, ni `Range`: no hay forma más barata de preguntarle si cambió.
COBERTURA = re.compile(r"^Cobertura temporal,(\d{4})-(\d{2})-", re.MULTILINE)

# Todo lo que escribe esta fuente vive bajo este directorio del repo de datos.
ZONA = "profeco"

# La fuente no publica nada anterior: 2023 no tiene bundle.
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
    def nombres(self) -> set[str]:
        """Los dos nombres con que la fuente ha bautizado el mismo archivo.

        Hasta 2025 el sufijo era `_01`/`_02` y desde 2026 es `_Q1`/`_Q2`. Se buscan los
        dos en vez de atar la convención al año, que es lo único que sigue sirviendo si
        la vuelven a cambiar.
        """
        return {
            f"{self.mes:02d}-{self.anio}_{self.q:02d}.csv",
            f"{self.mes:02d}-{self.anio}_q{self.q}.csv",
        }


def quincenas(hasta: date) -> list[Quincena]:
    """Todas las quincenas entre PRIMERA y el mes de `hasta`, publicadas o no."""
    return [
        Quincena(anio, mes, q)
        for anio in range(PRIMERA[0], hasta.year + 1)
        for mes in range(1, 13)
        for q in (1, 2)
        if (anio, mes, q) >= PRIMERA and (anio, mes) <= (hasta.year, hasta.month)
    ]


def pendientes(todas: list[Quincena], manifiesto: list[dict]) -> list[Quincena]:
    """Las que aún no tienen entrada en el manifiesto.

    Que Profeco republique una quincena corregida no se detecta aquí: exigiría rebajar
    los bundles enteros en cada corrida. Para eso está `--rehacer`, que la vuelve a
    procesar con un `intento` nuevo y conserva las dos versiones.
    """
    hechas = {e["quincena"] for e in manifiesto}
    return [q for q in todas if q.etiqueta not in hechas]


def baja_texto(url: str) -> str:
    """Baja un archivo chico —el listado, los metadatos— y lo devuelve como texto."""
    with tempfile.TemporaryDirectory() as tmp:
        ruta = Path(tmp) / "texto"
        if descarga(url, ruta) is None:
            raise RuntimeError(f"la fuente no sirvió {url}")
        return ruta.read_text(encoding="utf-8-sig", errors="replace")


def publicados(html: str) -> dict[int, str]:
    """{año: URL de su bundle}, según el listado."""
    return {int(anio): f"{BASE}/{ruta}" for ruta, anio in ENLACE_ANIO.findall(html)}


def url_metadatos(html: str) -> str:
    if (enlace := ENLACE_METADATOS.search(html)) is None:
        raise RuntimeError("el listado ya no trae el enlace de metadatos")
    return f"{BASE}/{enlace.group(1)}"


def cobertura(metadatos: str) -> tuple[int, int]:
    """El (año, mes) más reciente que la fuente declara publicado."""
    if (linea := COBERTURA.search(metadatos)) is None:
        raise RuntimeError("los metadatos ya no traen la cobertura temporal")
    return int(linea.group(1)), int(linea.group(2))


def miembro(nombres: list[str], q: Quincena) -> str | None:
    """La ruta de `q` dentro del bundle, o None si ese archivo no viene."""
    for nombre in nombres:
        if nombre.rsplit("/", 1)[-1].lower() in q.nombres:
            return nombre
    return None


def huella(ruta: Path) -> tuple[str, int]:
    """(sha256, bytes) del CSV, que es lo que el manifiesto promete poder reproducir."""
    digest, total = hashlib.sha256(), 0
    with ruta.open("rb") as f:
        while trozo := f.read(1 << 20):
            digest.update(trozo)
            total += len(trozo)
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
        destino / ZONA / "precios" / f"anio={q.anio}" / f"qqp_{nombre}",
        destino / ZONA / "tiendas" / f"anio={q.anio}" / f"tiendas_{nombre}",
    )


def escribe(df: pl.DataFrame, ruta: Path) -> None:
    ruta.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(ruta, compression="zstd")


def del_bundle(
    url: str, cola: list[Quincena], tmp: Path
) -> Iterator[tuple[Quincena, Path, str]]:
    """Baja el bundle del año y va entregando (quincena, csv, origen)."""
    bundle = tmp / "bundle"
    if descarga(url, bundle) is None:
        print(f"  la fuente no sirvió {url}")
        return
    if not zipfile.is_zipfile(bundle):
        raise RuntimeError(
            f"lo que sirvió {url} no es un zip. O el año viene como rar —2025 lo es— y "
            "hay que extraerlo a mano y pasarlo con --local, o el token cambió y el "
            "portal devolvió su página de error, que contesta 200 con HTML y no 404."
        )
    with zipfile.ZipFile(bundle) as zf:
        nombres = zf.namelist()
        for q in cola:
            if (nombre := miembro(nombres, q)) is None:
                print(f"  {q.etiqueta}: no viene en el bundle")
                continue
            crudo = tmp / "crudo.csv"
            with zf.open(nombre) as dentro, crudo.open("wb") as f:
                shutil.copyfileobj(dentro, f)
            yield q, crudo, f"{url}#{nombre}"


def del_local(
    raiz: Path, url: str, cola: list[Quincena]
) -> Iterator[tuple[Quincena, Path, str]]:
    """Los CSV ya extraídos a mano, que es la única vía para el año que viene en rar."""
    disponibles = {ruta.name.lower(): ruta for ruta in raiz.rglob("*.csv")}
    for q in cola:
        crudo = next((disponibles[n] for n in q.nombres if n in disponibles), None)
        if crudo is None:
            print(f"  {q.etiqueta}: no está en {raiz}")
            continue
        yield q, crudo, f"{url}#{crudo.name}"


def procesa(
    q: Quincena,
    crudo: Path,
    origen: str,
    productos: list[str],
    destino: Path,
    intento: int,
) -> dict:
    """Corta y escribe una quincena ya materializada. Devuelve su entrada de manifiesto."""
    sha, total = huella(crudo)
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
        "url_origen": origen,
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
        "--local",
        type=Path,
        metavar="CARPETA",
        help="usa los CSV ya extraídos de esa carpeta en vez de bajar el bundle",
    )
    cli.add_argument(
        "--rehacer",
        action="append",
        default=[],
        metavar="ETIQUETA",
        help="vuelve a procesar una quincena ya hecha, p. ej. 2025-11_q2",
    )
    args = cli.parse_args()

    productos = yaml.safe_load(args.objetivo.read_text(encoding="utf-8"))["producto"]
    manifiesto_ruta = args.destino / ZONA / "manifiesto.jsonl"
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

    html = baja_texto(LISTADO)
    bundles = publicados(html)
    hasta = cobertura(baja_texto(url_metadatos(html)))

    print(f"catálogo  : {productos}")
    print(f"destino   : {args.destino}")
    print(f"bundles   : {sorted(bundles)}, la fuente cubre hasta {hasta[0]}-{hasta[1]:02d}")
    print(f"pendientes: {len(falta)}, tope de la tanda: {tope}")

    # La sonda: lo que la fuente todavía no cubre no se baja para nada.
    cola = [q for q in cola if (q.anio, q.mes) <= hasta]

    intentos = {}
    for entrada in manifiesto:
        intentos[entrada["quincena"]] = max(
            intentos.get(entrada["quincena"], 0), entrada["intento"]
        )

    # El tope cuenta quincenas procesadas, no intentadas: saltarse las que no vienen en
    # el bundle es lo que evita que un hueco permanente en la fuente deje al cron atorado
    # antes de llegar a lo que sí salió.
    hechas = []
    manifiesto_ruta.parent.mkdir(parents=True, exist_ok=True)
    with manifiesto_ruta.open("a", encoding="utf-8") as f:
        for anio in sorted({q.anio for q in cola}):
            if len(hechas) >= tope:
                break
            del_anio = [q for q in cola if q.anio == anio]
            url = bundles.get(anio)
            if url is None and args.local is None:
                print(f"  {anio}: la fuente todavía no publica su bundle")
                continue
            with tempfile.TemporaryDirectory() as tmp:
                lote = (
                    del_local(args.local, url or LISTADO, del_anio)
                    if args.local
                    else del_bundle(url, del_anio, Path(tmp))
                )
                # `closing` no sobra: al cortar por el tope se sale del `for` sin agotar
                # el generador, y entonces el zip sigue abierto cuando el temporal se
                # va a borrar. En Windows eso es un PermissionError, no un descuido.
                with closing(lote):
                    for q, crudo, origen in lote:
                        if len(hechas) >= tope:
                            break
                        entrada = procesa(
                            q,
                            crudo,
                            origen,
                            productos,
                            args.destino,
                            intentos.get(q.etiqueta, 0) + 1,
                        )
                        f.write(json.dumps(entrada, ensure_ascii=False) + "\n")
                        f.flush()
                        hechas.append(q.etiqueta)

    print(f"procesadas: {len(hechas)} {hechas}")
    salida_actions(procesadas=" ".join(hechas))
    return 0


if __name__ == "__main__":
    sys.exit(main())
