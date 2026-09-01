"""Pruebas del versionado por hash, que es lo único que esta ingesta decide.

No tocan la red: `descarga` se sustituye por un archivo local con su sha real, porque
lo que hay que probar es qué hace el script con ese sha, no cómo lo bajó.
"""

import hashlib
import json

import ingesta_conasami as conasami

# Dos filas reales de sm_real_indice.csv, con encabezado.
CSV = "anio,mes,smg_nominal,smg_real,smgr_indice\n2024,1,258.8,193.78,225.93\n"
CSV_NUEVO = CSV + "2024,2,258.8,193.12,225.16\n"


def _baja(monkeypatch, contenido):
    """Sustituye la descarga por un archivo local. Devuelve el sha que verá el script."""
    crudo = contenido.encode("utf-8")

    def falsa(_url, destino):
        destino.write_bytes(crudo)
        return hashlib.sha256(crudo).hexdigest(), len(crudo)

    monkeypatch.setattr(conasami, "descarga", falsa)
    return hashlib.sha256(crudo).hexdigest()


def test_ruta_cuelga_de_la_fuente_y_versiona(tmp_path):
    assert conasami.ruta(tmp_path, "sm_real_indice", 1).relative_to(tmp_path).as_posix() == (
        "conasami/salarios/sm_real_indice.csv"
    )
    assert conasami.ruta(tmp_path, "sm_real_indice", 2).name == "sm_real_indice_v2.csv"


def test_ultima_toma_la_version_mayor():
    manifiesto = [
        {"archivo": "sm_real_indice", "version": 1, "sha256": "a"},
        {"archivo": "sm_real_indice", "version": 2, "sha256": "b"},
        {"archivo": "otro", "version": 9, "sha256": "c"},
    ]
    assert conasami.ultima(manifiesto, "sm_real_indice")["sha256"] == "b"
    assert conasami.ultima(manifiesto, "no_bajado") is None


def test_primera_bajada_es_v1_y_cuenta_filas(tmp_path, monkeypatch):
    sha = _baja(monkeypatch, CSV)
    entrada = conasami.procesa("sm_real_indice", tmp_path, [])

    assert entrada["version"] == 1
    assert entrada["sha256"] == sha
    assert entrada["filas"] == 1  # el encabezado no cuenta
    assert conasami.ruta(tmp_path, "sm_real_indice", 1).read_text(encoding="utf-8") == CSV


def test_mismo_sha_no_escribe_nada(tmp_path, monkeypatch):
    sha = _baja(monkeypatch, CSV)
    manifiesto = [{"archivo": "sm_real_indice", "version": 1, "sha256": sha}]

    assert conasami.procesa("sm_real_indice", tmp_path, manifiesto) is None
    assert not (tmp_path / "conasami").exists()


def test_sha_distinto_abre_v2_y_conserva_la_v1(tmp_path, monkeypatch):
    _baja(monkeypatch, CSV)
    conasami.procesa("sm_real_indice", tmp_path, [])

    previa = conasami.procesa("sm_real_indice", tmp_path, [])  # manifiesto vacío: v1 otra vez
    assert previa["version"] == 1

    _baja(monkeypatch, CSV_NUEVO)
    manifiesto = [{"archivo": "sm_real_indice", "version": 1, "sha256": previa["sha256"]}]
    entrada = conasami.procesa("sm_real_indice", tmp_path, manifiesto)

    assert entrada["version"] == 2
    assert entrada["filas"] == 2
    # La v1 sigue donde estaba: una versión nueva no pisa a la anterior.
    assert conasami.ruta(tmp_path, "sm_real_indice", 1).read_text(encoding="utf-8") == CSV
    assert conasami.ruta(tmp_path, "sm_real_indice", 2).read_text(encoding="utf-8") == CSV_NUEVO


def test_no_publicado_no_es_error(tmp_path, monkeypatch):
    monkeypatch.setattr(conasami, "descarga", lambda _url, _destino: None)
    assert conasami.procesa("sm_real_indice", tmp_path, []) is None


def test_la_entrada_del_manifiesto_es_una_linea_json(tmp_path, monkeypatch):
    _baja(monkeypatch, CSV)
    entrada = conasami.procesa("sm_real_indice", tmp_path, [])
    # Sin `quincena` ni `filas_filtradas`: esta fuente no tiene período ni corte.
    assert set(json.loads(json.dumps(entrada))) == {
        "url_origen",
        "sha256",
        "bytes",
        "filas",
        "archivo",
        "descargado_utc",
        "version",
    }
