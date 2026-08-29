"""Pruebas de los cortes y del recorrido de quincenas.

No tocan la red: `descarga` es lo único que sale, y lo que importa de ella —que un
error de HTTP se lea como "no publicada"— se prueba contra un error inyectado.
"""

from datetime import date
from pathlib import Path
from urllib.error import HTTPError

import ingesta
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


def test_etiqueta_y_url():
    q = ingesta.Quincena(2025, 11, 2)
    assert q.etiqueta == "2025-11_q2"
    assert q.url.endswith("programa_quien_es_quien_precios_2025/11-2025_02.csv")


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


def test_rutas_versionan_el_reintento():
    q = ingesta.Quincena(2025, 11, 2)
    precios, tiendas = ingesta.rutas(Path("datos"), q, 1)
    assert precios == Path("datos/precios/anio=2025/qqp_2025-11_q2.parquet")
    assert tiendas == Path("datos/tiendas/anio=2025/tiendas_2025-11_q2.parquet")

    reintento, _ = ingesta.rutas(Path("datos"), q, 2)
    assert reintento.name == "qqp_2025-11_q2_i2.parquet"


def test_manifiesto_vacio_si_no_existe(tmp_path):
    assert ingesta.leer_manifiesto(tmp_path / "no_existe.jsonl") == []


def test_lee_csv_absorbe_bom_y_crlf(tmp_path):
    ruta = tmp_path / "crudo.csv"
    ruta.write_bytes("﻿producto,precio\r\nPastelillos y Pan Dulce Empaquetado,20\r\n".encode())
    df = ingesta.lee_csv(ruta)
    assert df.columns == ["producto", "precio"]
    assert df["precio"].dtype == pl.String  # bronze no castea
    assert df["producto"][0] == "Pastelillos y Pan Dulce Empaquetado"


def _con_error(monkeypatch, codigo):
    def truena(*_args, **_kwargs):
        raise HTTPError("url", codigo, "vaya", {}, None)

    monkeypatch.setattr(ingesta.urllib.request, "urlopen", truena)


@pytest.mark.parametrize("codigo", ingesta.NO_PUBLICADA)
def test_descarga_lee_503_y_404_como_no_publicada(tmp_path, monkeypatch, codigo):
    _con_error(monkeypatch, codigo)
    assert ingesta.descarga("https://ejemplo/x.csv", tmp_path / "x.csv") is None


def test_descarga_no_se_traga_los_demas_errores(tmp_path, monkeypatch):
    # Un 403 —lo que devuelve el CDN si falta `Accept`— leído como "no publicada"
    # dejaría al pipeline sin bajar nada y sin quejarse.
    _con_error(monkeypatch, 403)
    with pytest.raises(HTTPError):
        ingesta.descarga("https://ejemplo/x.csv", tmp_path / "x.csv")


def test_descarga_pide_accept():
    assert ingesta.CABECERAS["Accept"] == "*/*"


def test_objetivo_yml_declara_el_corte():
    import yaml

    objetivo = yaml.safe_load((ingesta.RAIZ / "objetivo.yml").read_text(encoding="utf-8"))
    assert objetivo["producto"] == OBJETIVO


@pytest.mark.parametrize("intento", [1, 3])
def test_escribe_y_relee(tmp_path, intento):
    ruta, _ = ingesta.rutas(tmp_path, ingesta.Quincena(2024, 6, 1), intento)
    corte = ingesta.corte_precios(MUESTRA, OBJETIVO)
    ingesta.escribe(corte, ruta)
    assert pl.read_parquet(ruta).equals(corte)
