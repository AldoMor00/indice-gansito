"""Despliega los items de fabric/ al workspace destino con fabric-cicd.

Corre en GitHub Actions, no en Fabric. `azure/login` autentica por OIDC y deja una
sesion de az CLI; AzureCliCredential es la que la recoge. Es la credencial que
documenta fabric-cicd para este escenario: el fallback de DefaultAzureCredential
dejo de estar soportado en la version 1.0.0.

Ver docs/decisiones.md.
"""

import os
import sys
from pathlib import Path

from azure.identity import AzureCliCredential
from fabric_cicd import FabricWorkspace, publish_all_items

# Los tipos de item que fabric-cicd soporta han ido cambiando entre versiones.
# Esta lista se confirma contra la version instalada antes de agregar un item nuevo.
#
# Lakehouse queda fuera a proposito: los de prod se crean a mano y son los duenos de
# los datos, asi que el deploy no debe administrarlos. Ademas la carpeta que serializa
# git integration incluye shortcuts.metadata.json, y publicarla le empujaria a prod el
# shortcut con el que dev lee de prod.
TIPOS_EN_ALCANCE = [
    "Notebook",
    "DataPipeline",
    "SemanticModel",
    "Report",
]


def main() -> int:
    workspace_id = os.environ["FABRIC_WORKSPACE_ID"]
    entorno = os.environ["FABRIC_ENVIRONMENT"]
    directorio = Path(__file__).resolve().parent.parent / "fabric"

    en_alcance = sorted(
        p.name
        for p in directorio.iterdir()
        if p.is_dir() and p.name.rsplit(".", 1)[-1] in TIPOS_EN_ALCANCE
    )
    print(f"workspace : {workspace_id}")
    print(f"entorno   : {entorno}")
    print(f"commit    : {os.environ.get('GITHUB_SHA', 'local')}")
    print(f"en alcance: {len(en_alcance)} items {en_alcance}")

    workspace = FabricWorkspace(
        workspace_id=workspace_id,
        environment=entorno,
        repository_directory=str(directorio),
        item_type_in_scope=TIPOS_EN_ALCANCE,
        token_credential=AzureCliCredential(),
    )

    # Deliberadamente NO se llama a unpublish_all_orphan_items: borrar un item
    # huerfano puede llevarse un lakehouse y sus datos.
    publish_all_items(workspace)
    return 0


if __name__ == "__main__":
    sys.exit(main())
