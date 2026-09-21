"""Genera en web/img/ una variante WebP de cada captura de docs/img/, y la tarjeta de enlace.

Las capturas originales son PNG de 2880 x 1800 (1440 x 900 a 2x). La página no las carga
así: se recortan las que sólo interesan por el lienzo del reporte, se reducen a ANCHO_MAX y
se guardan en WebP. web/ queda autocontenido y es lo único que sube a GitHub Pages.

Además arma `web/img/tarjeta.png`, la imagen de Open Graph que LinkedIn y Slack muestran al
pegar el enlace. Va en PNG y no en WebP porque LinkedIn no lo lee.

Uso, desde la raíz del repo:
    uv run --no-project --python 3.13 --with pillow scripts/imagenes_web.py
"""

from pathlib import Path

from PIL import Image, ImageDraw

RAIZ = Path(__file__).resolve().parent.parent
ORIGEN = RAIZ / "docs" / "img"
DESTINO = RAIZ / "web" / "img"
ANCHO_MAX = 2000
CALIDAD = 80

# La tarjeta de Open Graph: 1200 x 630 es lo que piden Facebook, LinkedIn y Slack. El lienzo
# del reporte es más cuadrado que eso, así que se ajusta dentro y el sobrante queda en el
# fondo de la página, como si la tarjeta fuera una captura enmarcada.
TARJETA = (1200, 630)
TARJETA_MARGEN = 28
TARJETA_FUENTE = "02_responde_p1-el-numero.png"
FONDO = (250, 247, 242)
LINEA = (214, 214, 214)

# Recorte (izquierda, arriba, derecha, abajo) en píxeles del original. Las capturas que no
# están aquí van completas.
LIENZO_REPORTE = (511, 313, 2812, 1625)
CELDA_NOTEBOOK = (864, 295, 2808, 1240)
LIENZO_MODELO = (137, 371, 2254, 1656)
LIENZO_PIPELINE = (144, 288, 2866, 1512)
HUB_MONITOREO = (677, 122, 2822, 1786)
CELDA_COMPUERTA = (763, 418, 2808, 1066)
CORRIDA_ACTIONS = (0, 0, 2880, 1460)
PANEL_GITHUB = (554, 29, 2866, 1786)
LAKEHOUSE = (151, 418, 2866, 1786)
CONSULTA_LAKEHOUSE = (151, 230, 2866, 1238)
RECORTES = {
    "02_responde_p1-el-numero.png": LIENZO_REPORTE,
    "02_responde_p2-donde.png": LIENZO_REPORTE,
    "02_responde_p3-quien-subio.png": LIENZO_REPORTE,
    "02_responde_p4-poder-adquisitivo.png": LIENZO_REPORTE,
    "04_como-corre_monitoring-hub-prod.png": HUB_MONITOREO,
    "04_como-corre_pl-bronze-sesion.png": LIENZO_PIPELINE,
    "04_como-corre_pl-master-corrida.png": LIENZO_PIPELINE,
    "05_adentro_compuerta-truena.png": CELDA_COMPUERTA,
    "05_adentro_fila-bronze.png": CELDA_NOTEBOOK,
    "05_adentro_fila-silver.png": CELDA_NOTEBOOK,
    "05_adentro_fila-gold.png": CELDA_NOTEBOOK,
    "06_donde-vive_explorador-lh-gold.png": LAKEHOUSE,
    "06_donde-vive_historial-hechos-precios.png": CONSULTA_LAKEHOUSE,
    "06_donde-vive_vista-modelo-relaciones.png": LIENZO_MODELO,
    "07_metodo_p5-metodo-y-cobertura.png": LIENZO_REPORTE,
    "08_construye_corrida-deploy.png": CORRIDA_ACTIONS,
    "09_no-prod_decisiones-render.png": PANEL_GITHUB,
}


def tarjeta() -> None:
    """La captura de la primera página del reporte, centrada sobre el fondo de la página."""
    im = Image.open(ORIGEN / TARJETA_FUENTE).convert("RGB").crop(RECORTES[TARJETA_FUENTE])
    caja = (TARJETA[0] - 2 * TARJETA_MARGEN, TARJETA[1] - 2 * TARJETA_MARGEN)
    escala = min(caja[0] / im.width, caja[1] / im.height)
    im = im.resize((round(im.width * escala), round(im.height * escala)), Image.LANCZOS)

    lienzo = Image.new("RGB", TARJETA, FONDO)
    origen = ((TARJETA[0] - im.width) // 2, (TARJETA[1] - im.height) // 2)
    lienzo.paste(im, origen)
    ImageDraw.Draw(lienzo).rectangle(
        (origen[0] - 1, origen[1] - 1, origen[0] + im.width, origen[1] + im.height),
        outline=LINEA,
    )
    salida = DESTINO / "tarjeta.png"
    lienzo.save(salida, "PNG", optimize=True)
    print(f"{salida.name}  {TARJETA[0]}x{TARJETA[1]}  {salida.stat().st_size // 1024} KB")


def main() -> None:
    DESTINO.mkdir(exist_ok=True)
    for png in sorted(ORIGEN.glob("*.png")):
        im = Image.open(png).convert("RGB")
        if png.name in RECORTES:
            im = im.crop(RECORTES[png.name])
        if im.width > ANCHO_MAX:
            im = im.resize((ANCHO_MAX, round(im.height * ANCHO_MAX / im.width)), Image.LANCZOS)
        salida = DESTINO / png.with_suffix(".webp").name
        im.save(salida, "WEBP", quality=CALIDAD, method=6)
        print(f"{salida.name}  {im.width}x{im.height}  {salida.stat().st_size // 1024} KB")
    tarjeta()


if __name__ == "__main__":
    main()
