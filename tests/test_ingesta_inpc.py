"""Pruebas de la ingesta del INPC.

No tocan la red: `descarga` se prueba en `test_fuente.py`. Lo que importa aquí es que el
token no se filtre al manifiesto y que un cuerpo que no trae observaciones no entre al
repo callado.
"""

import json

import ingesta_inpc as ingesta
import pytest

# Un recorte de lo que sirve la API, con la forma completa.
RESPUESTA = {
    "Header": {"Name": "Datos compactos BIE", "Email": "atencion.usuarios@inegi.org.mx"},
    "Series": [
        {
            "INDICADOR": "910420",
            "FREQ": "15",
            "UNIT": "1051",
            "LASTUPDATE": "09/09/2026 12:00:00 a. m.",
            "OBSERVATIONS": [
                {"TIME_PERIOD": "2026/08/02", "OBS_VALUE": "145.53100000"},
                {"TIME_PERIOD": "2026/08/01", "OBS_VALUE": "145.39300000"},
                {"TIME_PERIOD": "2024/01/01", "OBS_VALUE": "133.34000000"},
            ],
        }
    ],
}


def _sirve(tmp_path, cuerpo):
    ruta = tmp_path / "crudo.json"
    ruta.write_text(json.dumps(cuerpo), encoding="utf-8")
    return ruta


def test_la_url_lleva_el_indicador_la_fuente_y_la_geografia():
    url = ingesta.url_de("SECRETO")
    assert f"/INDICATOR/{ingesta.INDICADOR}/es/{ingesta.GEO}/false/{ingesta.FUENTE}/" in url
    # `false` y no `true`: con `true` la API devuelve sólo la última observación.
    assert "/false/" in url


def test_el_token_no_entra_al_manifiesto():
    # Va en la URL, así que la línea del manifiesto tiene que guardarla con el hueco.
    guardada = ingesta.url_de(ingesta.MASCARA)
    assert "SECRETO" not in guardada
    assert ingesta.MASCARA in guardada


def test_observaciones_cuenta_y_saca_el_ultimo_periodo(tmp_path):
    n, ultimo, actualizado = ingesta.observaciones(_sirve(tmp_path, RESPUESTA))
    assert (n, ultimo) == (3, "2026/08/02")
    assert actualizado.startswith("09/09/2026")


def test_un_cuerpo_sin_observaciones_truena(tmp_path):
    # La API contesta 400 cuando el token o el id están mal y eso lo detiene `descarga`.
    # Esto cubre lo demás: un 200 con un cuerpo que no es la serie.
    vacia = {"Series": [{"INDICADOR": "910420", "OBSERVATIONS": []}]}
    for cuerpo in ({}, {"Series": []}, vacia):
        with pytest.raises(RuntimeError, match="observaciones"):
            ingesta.observaciones(_sirve(tmp_path, cuerpo))


def test_la_version_versiona_el_archivo(tmp_path):
    assert ingesta.ruta(tmp_path, 1).name == "inpc_quincenal.json"
    assert ingesta.ruta(tmp_path, 3).name == "inpc_quincenal_v3.json"
    assert ingesta.ruta(tmp_path, 1).parent == tmp_path / ingesta.ZONA / "serie"


def test_ultima_se_queda_con_la_version_mas_alta():
    manifiesto = [{"version": 1, "sha256": "a"}, {"version": 2, "sha256": "b"}]
    assert ingesta.ultima(manifiesto)["sha256"] == "b"
    assert ingesta.ultima([]) is None
