"""Pruebas de los cortes y del recorrido de quincenas.

No tocan la red: lo único que sale es `descarga`, que ya no vive aquí —se comparte con
la ingesta de CONASAMI— y se prueba en `test_fuente.py`.
"""

import codecs
import zipfile
import zlib
from datetime import date
from pathlib import Path

import ingesta_profeco as ingesta
import polars as pl
import pytest

# Una muestra con los defectos reales del CSV: `S/m` como marca de granel, el mismo
# SKU en dos catálogos, y dos filas de la misma tienda.
MUESTRA = pl.DataFrame(
    {
        "producto": [
            "Pastelillos y Pan Dulce Empaquetado",
            "Pastelillos y Pan Dulce Empaquetado",
            "Pan Dulce",
            "Pilas Eléctricas",
        ],
        "presentacion": [
            "Paquete con 1 Gansito (50 Gr.)",
            "Paquete con 1 Gansito (50 Gr.)",
            "Concha. Pieza de 68 a 90 Gr.",
            "Paquete con 2. Aa",
        ],
        "marca": ["Marinela", "Marinela", "S/m", "Duracell"],
        "categoria": ["Pan", "Pan", "Pan", "Accesorios Domesticos"],
        "catalogo": ["Basicos", "Mercados", "Basicos", "Basicos"],
        "precio": ["20", "22", "18", "95"],
        "fecha_registro": ["2025/11/16"] * 4,
        "cadena_comercial": ["Soriana", "Soriana", "Mercado", "Soriana"],
        "giro": ["Supermercado", "Supermercado", "Mercado", "Supermercado"],
        "nombre_comercial": [
            "Soriana Centro",
            "Soriana Centro",
            "Mercado Juárez",
            "Soriana Sur",
        ],
        "direccion": ["Calle 1", "Calle 1", "Calle 2", "Calle 3"],
        "estado": ["Jalisco"] * 4,
        "municipio": ["Guadalajara"] * 4,
        "latitud": ["20.67", "20.67", "20.68", "20.69"],
        "longitud": ["-103.35", "-103.35", "-103.36", "-103.37"],
    }
)

OBJETIVO = ["Pastelillos y Pan Dulce Empaquetado"]


def _li(token: str, texto: str) -> str:
    return f'<li><a href="file.php?t={token}">{texto}</a></li>'


# El listado como lo sirve el portal, con la basura de plantilla incluida: un `</ul>`
# suelto y los enlaces en desorden, 2025 antes que 2026.
LISTADO = "\n".join(
    [
        "                     </ul>",
        _li("b9540b181657c2bc7735892091e81f96", "Quien es Quien en los Precios 2025"),
        _li("9d62040eae6dcc63e36b8ac821427647", "Quien es Quien en los Precios 2026"),
        _li("42ed7dad4da507b9d536d9b737e7912d", "Metadatos dataset"),
        _li("2de3505b2d37e7db72557a09262d95c5", "Quien es Quien en los Precios 2024"),
        _li("2de3505b2d37e7db72557a09262d95c7", "Diccionario de datos dataset"),
    ]
)

METADATOS = (
    "Metadato,Descripción\n"
    'Título,"Programa Quién es Quién en los Precios, Julio de 2026."\n'
    "Cobertura temporal,2026-07-01 a 2026-07-31\n"
    "Temporalidad de actualización,Mensual\n"
)


def test_etiqueta_y_los_dos_nombres_de_archivo():
    q = ingesta.Quincena(2025, 11, 2)
    assert q.etiqueta == "2025-11_q2"
    # Hasta 2025 el sufijo era `_02`; desde 2026 es `_Q2`. Se aceptan los dos.
    assert q.nombres == {"11-2025_02.csv", "11-2025_q2.csv"}


def test_publicados_arma_un_bundle_por_anio():
    assert ingesta.publicados(LISTADO) == {
        2024: f"{ingesta.BASE}/file.php?t=2de3505b2d37e7db72557a09262d95c5",
        2025: f"{ingesta.BASE}/file.php?t=b9540b181657c2bc7735892091e81f96",
        2026: f"{ingesta.BASE}/file.php?t=9d62040eae6dcc63e36b8ac821427647",
    }


def test_publicados_no_confunde_el_diccionario_ni_los_metadatos():
    # Los dos cuelgan del mismo `file.php` y sólo el texto del enlace los distingue.
    assert ingesta.url_metadatos(LISTADO).endswith("t=42ed7dad4da507b9d536d9b737e7912d")


def test_el_ano_que_falta_simplemente_no_esta():
    # Es todo lo que el cambio de año necesita: 2027 entra solo cuando aparezca su `li`.
    assert 2027 not in ingesta.publicados(LISTADO)


def test_plantilla_cambiada_deja_el_mapa_vacio():
    # Sin `li` que casar no hay de dónde sacar el token, y eso tiene que notarse.
    assert ingesta.publicados("<p>ya no hay listado</p>") == {}
    with pytest.raises(RuntimeError):
        ingesta.url_metadatos("<p>ya no hay listado</p>")


def test_cobertura_lee_el_mes_mas_reciente():
    assert ingesta.cobertura(METADATOS) == (2026, 7)


def test_cobertura_se_queja_si_el_metadato_cambia():
    with pytest.raises(RuntimeError):
        ingesta.cobertura("Metadato,Descripción\nAutor,Profeco\n")


def test_miembro_reconoce_las_dos_convenciones():
    nombres = ["QQP_2026/", "QQP_2026/07-2026_Q1.csv", "QQP_2026/07-2026_Q2.csv"]
    assert ingesta.miembro(nombres, ingesta.Quincena(2026, 7, 2)) == "QQP_2026/07-2026_Q2.csv"

    viejos = ["QQP_2025/11-2025_01.csv", "QQP_2025/11-2025_02.csv"]
    assert ingesta.miembro(viejos, ingesta.Quincena(2025, 11, 2)) == "QQP_2025/11-2025_02.csv"


def test_miembro_es_none_si_la_quincena_no_viene():
    nombres = ["QQP_2026/07-2026_Q1.csv", "QQP_2026/07-2026_Q2.csv"]
    assert ingesta.miembro(nombres, ingesta.Quincena(2026, 8, 1)) is None


def test_quincenas_arranca_en_2024_y_no_pasa_del_mes():
    todas = ingesta.quincenas(date(2025, 11, 30))
    assert todas[0] == ingesta.Quincena(2024, 1, 1)
    assert todas[-1] == ingesta.Quincena(2025, 11, 2)
    # 23 meses completos, dos quincenas cada uno
    assert len(todas) == 46


def test_pendientes_descuenta_lo_del_manifiesto():
    todas = ingesta.quincenas(date(2024, 2, 15))
    manifiesto = [{"quincena": "2024-01_q1", "intento": 1}]
    assert [q.etiqueta for q in ingesta.pendientes(todas, manifiesto)] == [
        "2024-01_q2",
        "2024-02_q1",
        "2024-02_q2",
    ]


def test_corte_precios_conserva_los_dos_catalogos():
    corte = ingesta.corte_precios(MUESTRA, OBJETIVO)
    assert corte.height == 2
    assert set(corte["catalogo"]) == {"Basicos", "Mercados"}
    assert corte.columns == ingesta.COLUMNAS_PRECIOS


def test_corte_precios_tira_las_columnas_que_no_son_del_contrato():
    # Junio de 2026: Profeco publicó sus llaves internas y las quitó en julio. Bronze
    # escribe con mergeSchema apagado, así que una columna de más tumba la corrida.
    con_llaves = MUESTRA.with_columns(
        pl.lit("44375").alias("folio"),
        pl.lit("8").alias("cv_producto"),
        pl.lit("2").alias("cv_marca"),
    )
    corte = ingesta.corte_precios(con_llaves, OBJETIVO)
    assert corte.columns == ingesta.COLUMNAS_PRECIOS
    # Y el corte es el mismo que sin ellas: sólo sobraban columnas, no filas.
    assert corte.equals(ingesta.corte_precios(MUESTRA, OBJETIVO))


def test_corte_precios_truena_si_falta_una_del_contrato():
    with pytest.raises(RuntimeError, match="precio"):
        ingesta.corte_precios(MUESTRA.drop("precio"), OBJETIVO)


def test_corte_tiendas_sale_del_archivo_completo():
    # La tienda del Mercado no vende pastelillos y aun así tiene que estar.
    tiendas = ingesta.corte_tiendas(MUESTRA)
    assert tiendas.height == 3
    assert "Mercado Juárez" in set(tiendas["nombre_comercial"])
    assert tiendas.columns == ingesta.COLUMNAS_TIENDA


def test_los_cortes_son_deterministas():
    revuelta = MUESTRA.sample(fraction=1.0, shuffle=True, seed=7)
    assert ingesta.corte_precios(revuelta, OBJETIVO).equals(
        ingesta.corte_precios(MUESTRA, OBJETIVO)
    )
    assert ingesta.corte_tiendas(revuelta).equals(ingesta.corte_tiendas(MUESTRA))


def test_rutas_cuelgan_de_la_fuente_y_versionan_el_reintento():
    q = ingesta.Quincena(2025, 11, 2)
    precios, tiendas = ingesta.rutas(Path("datos"), q, 1)
    assert precios == Path("datos/profeco/precios/anio=2025/qqp_2025-11_q2.parquet")
    assert tiendas == Path("datos/profeco/tiendas/anio=2025/tiendas_2025-11_q2.parquet")

    reintento, _ = ingesta.rutas(Path("datos"), q, 2)
    assert reintento.name == "qqp_2025-11_q2_i2.parquet"


TEXTO = "producto,presentacion\nPastelillos,Paquete 280 Gr. Panqué Nuez\n"


def _archivo(tmp_path, nombre, contenido: bytes):
    ruta = tmp_path / nombre
    ruta.write_bytes(contenido)
    return ruta


def test_codificacion_reconoce_utf8_con_y_sin_bom(tmp_path):
    assert ingesta.codificacion(_archivo(tmp_path, "a.csv", TEXTO.encode("utf-8"))) == "utf-8"
    con_bom = codecs.BOM_UTF8 + TEXTO.encode("utf-8")
    assert ingesta.codificacion(_archivo(tmp_path, "b.csv", con_bom)) == "utf-8"


def test_codificacion_cae_a_cp1252(tmp_path):
    # Mayo de 2026: cp1252 y sin BOM, los dos únicos de los 62.
    ruta = _archivo(tmp_path, "c.csv", TEXTO.encode("cp1252"))
    assert ingesta.codificacion(ruta) == "cp1252"


def test_un_utf8_sin_bom_no_acaba_leido_como_cp1252(tmp_path):
    # Lo que hace que esto no sea una cascada de las que fallan en silencio: al respaldo
    # sólo se llega cuando el archivo ya demostró que no es utf-8. El BOM no se mira,
    # porque es opcional y su ausencia no significa nada.
    sin_bom = _archivo(tmp_path, "d.csv", TEXTO.encode("utf-8"))
    assert not sin_bom.read_bytes().startswith(codecs.BOM_UTF8)
    assert ingesta.codificacion(sin_bom) == "utf-8"


def test_codificacion_truena_si_no_es_ninguna_de_las_dos(tmp_path):
    # 0x81 no es utf-8 válido y es uno de los cinco bytes que cp1252 tampoco acepta.
    ruta = _archivo(tmp_path, "e.csv", b"producto\n\x81\n")
    with pytest.raises(RuntimeError, match="no decodifica"):
        ingesta.codificacion(ruta)


def test_lee_csv_da_lo_mismo_en_las_dos_codificaciones(tmp_path):
    # Leer cp1252 como utf-8 lossy no truena: cambia cada acento por U+FFFD, y eso llegó
    # hasta la compuerta de silver disfrazado de un SKU nuevo.
    for nombre, codec in (("utf8.csv", "utf-8"), ("cp.csv", "cp1252")):
        ruta = _archivo(tmp_path, nombre, TEXTO.encode(codec))
        df = ingesta.lee_csv(ruta, ingesta.codificacion(ruta))
        assert df["presentacion"][0] == "Paquete 280 Gr. Panqué Nuez"
        assert "�" not in df["presentacion"][0]


def test_lee_csv_absorbe_bom_y_crlf(tmp_path):
    ruta = tmp_path / "crudo.csv"
    ruta.write_bytes("﻿producto,precio\r\nPastelillos y Pan Dulce Empaquetado,20\r\n".encode())
    df = ingesta.lee_csv(ruta)
    assert df.columns == ["producto", "precio"]
    assert df["precio"].dtype == pl.String  # bronze no castea
    assert df["producto"][0] == "Pastelillos y Pan Dulce Empaquetado"


def test_objetivo_yml_declara_el_corte():
    import yaml

    objetivo = yaml.safe_load((ingesta.RAIZ / "objetivo.yml").read_text(encoding="utf-8"))
    assert objetivo["producto"] == OBJETIVO


def _bundle_falso(contenido: dict[str, str]):
    """Reemplaza la descarga por un zip armado en memoria. No sale a la red."""

    def escribe(_url, destino):
        with zipfile.ZipFile(destino, "w") as zf:
            for nombre, texto in contenido.items():
                zf.writestr(nombre, texto)
        return "sha", destino.stat().st_size

    return escribe


def test_del_bundle_extrae_lo_que_viene_y_se_salta_lo_que_no(tmp_path, monkeypatch):
    monkeypatch.setattr(
        ingesta,
        "descarga",
        _bundle_falso({"QQP_2026/07-2026_Q2.csv": "producto,precio\nGansito,20\n"}),
    )
    cola = [ingesta.Quincena(2026, 7, 1), ingesta.Quincena(2026, 7, 2)]
    lote = list(ingesta.del_bundle("https://ejemplo/bundle", cola, {}, tmp_path))

    assert [q.etiqueta for q, _, _ in lote] == ["2026-07_q2"]
    _, crudo, origen = lote[0]
    assert crudo.read_text(encoding="utf-8").startswith("producto,precio")
    assert origen == "https://ejemplo/bundle#QQP_2026/07-2026_Q2.csv"


def test_del_bundle_se_salta_lo_que_no_cambio_y_entrega_lo_reescrito(tmp_path, monkeypatch):
    texto = "producto,precio\nGansito,20\n"
    monkeypatch.setattr(
        ingesta,
        "descarga",
        _bundle_falso(
            {
                "QQP_2026/07-2026_Q1.csv": texto,
                "QQP_2026/07-2026_Q2.csv": texto,
            }
        ),
    )
    intacta, reescrita = ingesta.Quincena(2026, 7, 1), ingesta.Quincena(2026, 7, 2)
    sellos = {
        # La q1 con su sello real: el bundle la trae igual y no hay que tocarla.
        intacta.etiqueta: (len(texto), zlib.crc32(texto.encode())),
        # La q2 con el CRC32 de cuando el precio decía 21: mismo tamaño, otro contenido.
        reescrita.etiqueta: (len(texto), zlib.crc32(texto.replace("20", "21").encode())),
    }
    lote = list(
        ingesta.del_bundle("https://ejemplo/bundle", [intacta, reescrita], sellos, tmp_path)
    )

    assert [q.etiqueta for q, _, _ in lote] == ["2026-07_q2"]


def test_del_bundle_truena_si_lo_que_bajo_no_es_zip(tmp_path, monkeypatch):
    # Las dos formas de llegar aquí: el año que viene como rar, y la página de error del
    # portal, que contesta 200 con HTML en vez de 404 cuando el token no existe.
    def html(_url, destino):
        destino.write_bytes(b"<script> alert('Documento no disponible'); </script>")
        return "sha", destino.stat().st_size

    monkeypatch.setattr(ingesta, "descarga", html)
    cola = [ingesta.Quincena(2026, 7, 2)]
    with pytest.raises(RuntimeError, match="--local"):
        list(ingesta.del_bundle("https://ejemplo/bundle", cola, {}, tmp_path))


def test_cambio_usa_el_tamano_y_el_crc_cuando_lo_hay():
    # Tamaño distinto: no hace falta mirar nada más.
    assert ingesta.cambio((100, 111), (200, 111)) is True
    # Mismo tamaño y mismo CRC32: intacta.
    assert ingesta.cambio((100, 111), (100, 111)) is False
    # Mismo tamaño y otro CRC32: es justo el caso que el tamaño solo no ve.
    assert ingesta.cambio((100, 222), (100, 111)) is True


def test_cambio_sin_crc_de_algun_lado_se_queda_en_el_tamano():
    # Las líneas de manifiesto anteriores a que se guardara el CRC32, y los CSV de
    # `--local`, donde no hay directorio central del que leerlo.
    assert ingesta.cambio((100, 222), (100, None)) is False
    assert ingesta.cambio((100, None), (100, 111)) is False
    assert ingesta.cambio((100, None), (200, None)) is True


def test_sellos_se_queda_con_el_ultimo_intento():
    manifiesto = [
        {"quincena": "2026-01_q1", "bytes": 10, "crc32": 111, "intento": 1},
        {"quincena": "2026-01_q1", "bytes": 20, "crc32": 222, "intento": 2},
        {"quincena": "2025-11_q2", "bytes": 30, "intento": 1},
    ]
    # La reescrita vale por su última versión, y la línea vieja no trae CRC32.
    assert ingesta.sellos_de(manifiesto) == {
        "2026-01_q1": (20, 222),
        "2025-11_q2": (30, None),
    }


def test_del_local_encuentra_el_csv_extraido_a_mano(tmp_path):
    # La vía del año que la fuente sirve como rar: el CSV ya está en disco, anidado como
    # lo deja el extractor.
    (tmp_path / "QQP_2025").mkdir()
    csv = tmp_path / "QQP_2025" / "12-2025_02.csv"
    csv.write_text("producto,precio\n", encoding="utf-8")

    cola = [ingesta.Quincena(2025, 12, 2), ingesta.Quincena(2025, 12, 1)]
    lote = list(ingesta.del_local(tmp_path, "https://ejemplo/bundle", cola, {}))

    # La que no está se salta sin tronar; la que sí, viaja con su origen.
    assert [(q.etiqueta, ruta) for q, ruta, _ in lote] == [("2025-12_q2", csv)]
    assert lote[0][2] == "https://ejemplo/bundle#12-2025_02.csv"


@pytest.mark.parametrize("intento", [1, 3])
def test_escribe_y_relee(tmp_path, intento):
    ruta, _ = ingesta.rutas(tmp_path, ingesta.Quincena(2024, 6, 1), intento)
    corte = ingesta.corte_precios(MUESTRA, OBJETIVO)
    ingesta.escribe(corte, ruta)
    assert pl.read_parquet(ruta).equals(corte)
