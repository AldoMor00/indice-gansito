"""Lo que comparten las ingestas de las dos fuentes: bajar un archivo y leer el
manifiesto.

Nada más vive aquí, porque nada más se repite. Profeco entrega lotes grandes e
inmutables por quincena y se corta en la puerta; CONASAMI entrega un archivo chico que
se reescribe y se guarda entero. Ver docs/decisiones.md.
"""

from __future__ import annotations

import hashlib
import json
import os
import urllib.error
import urllib.request
from pathlib import Path

# El host contesta 403 si la petición no trae `Accept` (urllib no lo manda por su
# cuenta), y Akamai además bloquea a clientes no-navegador aunque lo traigan
# (decisión #44): de ahí que lo mismo funcione en el navegador y aquí no.
CABECERAS = {
    "Accept": "*/*",
    "User-Agent": "indice-gansito (+https://github.com/AldoMor00/indice-gansito)",
}

# Lo que la fuente devuelve por un archivo que no existe. Responde 503, no 404, y no
# hay forma de distinguirlo de una caída real; el 404 va por si algún día lo arreglan.
NO_PUBLICADA = (404, 503)


class Bloqueada(urllib.error.HTTPError):
    """El host contestó 403: no deja entrar, que no es lo mismo que "no publicado".

    Es subclase de HTTPError para que quien no la distinga siga tronando como antes;
    quien sí (CONASAMI, decisión #44) la declara "bloqueado" y sale en verde.
    """


def descarga(url: str, destino: Path) -> tuple[str, int] | None:
    """Baja `url` a `destino`. Devuelve (sha256, bytes), o None si no está publicada.

    Un 403 se propaga como `Bloqueada`: leerlo como "todavía no sale" dejaría al
    pipeline sin bajar nada y sin quejarse. La tanda es reanudable, así que tronar es
    barato —el manifiesto ya tiene lo que alcanzó a escribir.
    """
    digest = hashlib.sha256()
    total = 0
    peticion = urllib.request.Request(url, headers=CABECERAS)
    try:
        with (
            urllib.request.urlopen(peticion, timeout=180) as respuesta,
            destino.open("wb") as f,
        ):
            while trozo := respuesta.read(1 << 20):
                digest.update(trozo)
                f.write(trozo)
                total += len(trozo)
    except urllib.error.HTTPError as e:
        if e.code in NO_PUBLICADA:
            return None
        if e.code == 403:
            raise Bloqueada(e.url, e.code, e.msg, e.hdrs, e.fp) from e
        raise
    return digest.hexdigest(), total


def leer_manifiesto(ruta: Path) -> list[dict]:
    if not ruta.exists():
        return []
    return [
        json.loads(linea)
        for linea in ruta.read_text(encoding="utf-8").splitlines()
        if linea.strip()
    ]


def salida_actions(**pares: str) -> None:
    """Deja lo procesado en GITHUB_OUTPUT para que el workflow decida si commitea."""
    if salida := os.environ.get("GITHUB_OUTPUT"):
        with Path(salida).open("a", encoding="utf-8") as f:
            for clave, valor in pares.items():
                f.write(f"{clave}={valor}\n")
