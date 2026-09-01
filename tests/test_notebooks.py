"""Guard de la regla dura #1: ningún notebook con lakehouse por defecto enlazado.

`nb_10_bronze` ya lo revisa en tiempo de ejecución, pero eso truena hasta que alguien
lo corre. Aquí se caza en el commit, que es donde el GUID entra al repo.
"""

import json
from pathlib import Path

import pytest

FABRIC = Path(__file__).resolve().parents[1] / "fabric"
NOTEBOOKS = sorted(FABRIC.glob("*.Notebook/notebook-content.py"))

# Un notebook enlazado, tal como lo commitea la git integration de Fabric.
ENLAZADO = """# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {"name": "synapse_pyspark"},
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "00000000-0000-0000-0000-000000000000",
# META       "default_lakehouse_name": "lh_silver"
# META     }
# META   }
# META }
"""


def bloques_meta(texto: str) -> list[dict]:
    """Los bloques `# META` del formato de git de Fabric, ya como dicts."""
    bloques, actual = [], []
    for linea in texto.splitlines():
        if linea.startswith("# META "):  # el separador `# METADATA ***` no lleva espacio
            actual.append(linea.removeprefix("# META "))
        elif actual:
            bloques.append(json.loads(" ".join(actual)))
            actual = []
    if actual:
        bloques.append(json.loads(" ".join(actual)))
    return bloques


def lakehouse_de(meta: dict) -> dict:
    return meta.get("dependencies", {}).get("lakehouse") or {}


def test_hay_notebooks_que_revisar():
    # Sin esto, un glob que deja de casar dejaría el guard en verde sin revisar nada.
    assert NOTEBOOKS


@pytest.mark.parametrize("ruta", NOTEBOOKS, ids=lambda r: r.parent.name)
def test_ningun_notebook_trae_lakehouse_por_defecto(ruta):
    enlazados = [
        lh
        for meta in bloques_meta(ruta.read_text(encoding="utf-8"))
        if (lh := lakehouse_de(meta))
    ]
    assert not enlazados, (
        f"{ruta.parent.name} trae lakehouse por defecto: {enlazados}. "
        "Retirarlo desde la UI de Fabric y volver a commitear."
    )


def test_el_guard_caza_un_lakehouse_enlazado():
    assert lakehouse_de(bloques_meta(ENLAZADO)[0])["default_lakehouse_name"] == "lh_silver"
