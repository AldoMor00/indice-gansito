"""Genera el clon import del reporte público a partir de sm_gansito y rpt_gansito.

El modelo de Fabric es Direct Lake y el público es import, leyendo los parquets de gold desde
`indice-gansito-datos/publico` (decisión #6). El clon es copia, no reescritura: medidas,
relaciones, tablas calculadas y páginas salen tal cual de fabric/, y lo único que cambia son
las particiones de las ocho tablas de gold.

El caparazón de publico/ lo creó Power BI Desktop, y de él se conservan el `.pbip`, los
`.platform` y `.pbi/`, menos la caché del modelo. Todo lo demás se borra y se vuelve a copiar
en cada corrida, así que un cambio hecho a mano en publico/ se pierde: los cambios van en
fabric/ y se vuelve a clonar.

    uv run --no-project scripts/clona_publico.py
"""

import json
import re
import shutil
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
SM_ORIGEN = RAIZ / "fabric" / "sm_gansito.SemanticModel"
RPT_ORIGEN = RAIZ / "fabric" / "rpt_gansito.Report"
SM_DESTINO = RAIZ / "publico" / "gansito_publico.SemanticModel"
RPT_DESTINO = RAIZ / "publico" / "gansito_publico.Report"

RAW = "https://raw.githubusercontent.com/AldoMor00/indice-gansito-datos/main/publico"
EXPRESION_DL = "DirectLake - lh_gold"

# La partición Direct Lake tal como la escribe Fabric.
PARTICION_DL = re.compile(
    r"\tpartition (?P<particion>\S+) = entity(?P<nl>\r?\n)"
    r"\t\tmode: directLake\r?\n"
    r"\t\tsource\r?\n"
    r"\t\t\tentityName: (?P<tabla>\S+)\r?\n"
    r"\t\t\tschemaName: dbo\r?\n"
    rf"\t\t\texpressionSource: '{EXPRESION_DL}'\r?\n"
)

# La partición import tal como la escribió Desktop en el caparazón, con la URL por tabla.
PARTICION_IMPORT = [
    "\tpartition {particion} = m",
    "\t\tmode: import",
    "\t\tsource =",
    "\t\t\t\tlet",
    '\t\t\t\t    Origen = Parquet.Document(Web.Contents("{url}"), '
    "[Compression=null, LegacyColumnNameEncoding=false, MaxDepth=null])",
    "\t\t\t\tin",
    "\t\t\t\t    Origen",
    "",
]

# `sourceLineageTag` amarra la tabla y sus columnas a la tabla Delta del lakehouse. En import no
# tiene a qué apuntar, y Desktop no lo escribe.
LINAJE_DL = re.compile(r"^\t+sourceLineageTag: .*\r?\n", re.MULTILINE)


def lee(ruta: Path) -> str:
    # `newline=""` conserva los CRLF del origen: el diff contra fabric/ queda en lo que cambió.
    return ruta.read_text(encoding="utf-8", newline="")


def escribe(ruta: Path, texto: str) -> None:
    ruta.write_text(texto, encoding="utf-8", newline="")


def a_import(match: re.Match) -> str:
    nl = match["nl"]
    url = f"{RAW}/{match['tabla']}.parquet"
    return nl.join(PARTICION_IMPORT).format(particion=match["particion"], url=url)


def como_desktop(columna: str) -> str:
    """Deja una columna import como la reescribe Desktop al refrescar, para que abrir y
    refrescar el clon no lo cambie.

    - `decimal` a `double`: el parquet trae `double` y Desktop ajusta el tipo al de Power
      Query. Con el cambio, un resumen automático se recalcula y pasaría a `sum`; se marca
      como fijado por el usuario para que se quede en `none`.
    - Una fecha del parquet se formatea `Long Date` y se anota como `Date`, como ya estaba
      `dim_mes.mes_inicio`.
    - La marca `changedProperty = DataType` Desktop la quita.
    """
    nl = "\r\n" if "\r\n" in columna else "\n"
    if f"\t\tdataType: decimal{nl}" in columna:
        columna = columna.replace(f"\t\tdataType: decimal{nl}", f"\t\tdataType: double{nl}")
        if f"\t\tsummarizeBy: none{nl}" in columna:
            columna = columna.replace(
                "annotation SummarizationSetBy = Automatic",
                "annotation SummarizationSetBy = User",
            )
    if f"\t\tdataType: dateTime{nl}" in columna and "UnderlyingDateTimeDataType" not in columna:
        columna = columna.replace("formatString: General Date", "formatString: Long Date")
        columna += f"\t\tannotation UnderlyingDateTimeDataType = Date{nl}{nl}"
    return columna.replace(f"\t\tchangedProperty = DataType{nl}{nl}", "")


def columnas_como_desktop(tabla: str) -> str:
    # Cada bloque empieza en su `column`, `measure`, `hierarchy` o `partition` y termina con
    # la línea en blanco que lo separa del siguiente.
    bloques = re.split(r"(?m)^(?=\t(?:column|measure|hierarchy|partition) )", tabla)
    return "".join(como_desktop(b) if b.startswith("\tcolumn ") else b for b in bloques)


def anota(texto: str, anotacion: str, valor: str) -> str:
    """Reemplaza el valor de una anotación de model.tmdl. `[^\\r\\n]` y no `.`: con CRLF, `.`
    se come el `\\r` y la línea queda en LF."""
    texto, n = re.subn(
        rf"(?m)^annotation {anotacion} = [^\r\n]*", f"annotation {anotacion} = {valor}", texto
    )
    if n != 1:
        raise RuntimeError(f"model.tmdl cambió de forma: no trae una sola `{anotacion}`")
    return texto


def clona_modelo() -> list[str]:
    """Copia la definición del modelo y cambia las particiones Direct Lake por import.
    Devuelve las tablas convertidas, en el orden de los archivos."""
    definicion = SM_DESTINO / "definition"
    shutil.rmtree(definicion, ignore_errors=True)
    # Desktop abre primero la caché y le aplica la definición encima. La del caparazón nació
    # es-ES y el modelo es en-US, y la cultura no se cambia en un modelo que ya tiene objetos:
    # sin la caché, Desktop arma el modelo desde el TMDL y la vuelve a crear al refrescar.
    (SM_DESTINO / ".pbi" / "cache.abf").unlink(missing_ok=True)
    shutil.copytree(SM_ORIGEN / "definition", definicion)

    # La única expresión compartida es la conexión a OneLake. Si aparece otra, el clon no
    # sabe qué hacer con ella y se detiene en vez de tirarla.
    expresiones = definicion / "expressions.tmdl"
    nombres = re.findall(r"^expression (.+?) =", lee(expresiones), re.MULTILINE)
    if nombres != [f"'{EXPRESION_DL}'"]:
        raise RuntimeError(
            f"expressions.tmdl trae algo más que la conexión Direct Lake: {nombres}"
        )
    expresiones.unlink()

    convertidas = []
    for archivo in sorted((definicion / "tables").glob("*.tmdl")):
        texto, n = PARTICION_DL.subn(a_import, lee(archivo))
        if n:
            convertidas.append(archivo.stem)
            texto = columnas_como_desktop(LINAJE_DL.sub("", texto))
        if "directLake" in texto or EXPRESION_DL in texto:
            raise RuntimeError(f"{archivo.name} sigue apuntando a Direct Lake")
        escribe(archivo, texto)

    modelo = definicion / "model.tmdl"
    texto = anota(lee(modelo), "PBI_QueryOrder", json.dumps(convertidas, separators=(",", ":")))
    texto = anota(texto, "PBI_ProTooling", '["DevMode"]')
    # Direct Lake ignora la fecha/hora automática; import la aplica y crea una tabla de
    # fechas oculta por cada columna de fecha.
    texto = anota(texto, "__PBI_TimeIntelligenceEnabled", "0")
    escribe(modelo, texto)

    return convertidas


def clona_reporte() -> None:
    """Copia páginas, tema y recursos; sólo cambia a qué modelo apunta el reporte."""
    for carpeta in ("definition", "StaticResources"):
        shutil.rmtree(RPT_DESTINO / carpeta, ignore_errors=True)
        shutil.copytree(RPT_ORIGEN / carpeta, RPT_DESTINO / carpeta)

    original = lee(RPT_ORIGEN / "definition.pbir")
    pbir = json.loads(original)
    pbir["datasetReference"]["byPath"]["path"] = f"../{SM_DESTINO.name}"
    texto = json.dumps(pbir, indent=2, ensure_ascii=False)
    if "\r\n" in original:
        texto = texto.replace("\n", "\r\n")
    escribe(RPT_DESTINO / "definition.pbir", texto)


def main() -> None:
    for caparazon in (SM_DESTINO / ".platform", RPT_DESTINO / ".platform"):
        if not caparazon.exists():
            raise RuntimeError(f"falta {caparazon}: el caparazón se crea con Power BI Desktop")

    tablas = clona_modelo()
    clona_reporte()
    print(f"{len(tablas)} tablas a import: {', '.join(tablas)}")


if __name__ == "__main__":
    main()
