"""Pruebas de lo que comparten las dos ingestas.

No tocan la red: lo que importa de `descarga` —que un error de HTTP se lea como "no
publicada" y que ningún otro se confunda con eso— va contra un error inyectado.
"""

from urllib.error import HTTPError

import fuente
import pytest


def _con_error(monkeypatch, codigo):
    def truena(*_args, **_kwargs):
        raise HTTPError("url", codigo, "vaya", {}, None)

    monkeypatch.setattr(fuente.urllib.request, "urlopen", truena)


@pytest.mark.parametrize("codigo", fuente.NO_PUBLICADA)
def test_descarga_lee_503_y_404_como_no_publicada(tmp_path, monkeypatch, codigo):
    _con_error(monkeypatch, codigo)
    assert fuente.descarga("https://ejemplo/x.csv", tmp_path / "x.csv") is None


def test_descarga_no_se_traga_los_demas_errores(tmp_path, monkeypatch):
    # Un 403 —lo que devuelve el CDN si falta `Accept`— leído como "no publicada"
    # dejaría al pipeline sin bajar nada y sin quejarse.
    _con_error(monkeypatch, 403)
    with pytest.raises(HTTPError):
        fuente.descarga("https://ejemplo/x.csv", tmp_path / "x.csv")


def test_descarga_pide_accept():
    assert fuente.CABECERAS["Accept"] == "*/*"


def test_manifiesto_vacio_si_no_existe(tmp_path):
    assert fuente.leer_manifiesto(tmp_path / "no_existe.jsonl") == []
