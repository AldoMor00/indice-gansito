# Convenciones del proyecto

## Commits

- **Sin coautoría ni firma de asistentes de IA.** Nada de `Co-Authored-By: Claude`
  ni de `Generated with` en mensajes de commit ni en cuerpos de PR.
- Mensajes en español, en imperativo: `Agrega...`, `Corrige...`, `Documenta...`.
- **No commitear ni abrir pull requests sin autorización explícita.** Editar archivos
  en el árbol de trabajo está bien; `git commit`, `git push` y `gh pr create` se piden
  antes.
- Cuerpos de PR breves: qué entra, qué no entra y por qué, y qué hace falta antes de
  mergear. Nada más.
- **Las `feat/*` y `fix/*` se mergean con squash**, y al mergear se borra la rama en
  local y en remoto. No se dejan ramas muertas colgando.
- **`dev` a `main` va con merge commit, nunca squash.** `dev` es permanente: el squash
  deja la base de comparación obsoleta y el merge siguiente sale vacío sin avisar.

## Python

- **Siempre a través de `uv`.** Nunca `pip`, `python -m venv` ni `python` a secas.
  - script suelto con dependencias: `uv run --with fabric-cicd scripts/deploy.py`
  - cuando haya proyecto: `uv sync`, `uv add`, `uv run pytest`
- Versión de Python: **3.13**, para igualar el runtime de Spark en Fabric. La excepción
  es `deploy.yml`, que corre `fabric-cicd` y no entra a Fabric: ahí manda lo que esa
  herramienta soporte.

## Documentación

- Las decisiones **se documentan**, pero cortas: un párrafo o dos por decisión en
  `docs/decisiones.md`. **Nada de un archivo por decisión.**
- Se documenta lo que ya se decidió, no lo que se planea. Sin documentación
  especulativa de fases que no han empezado.

## Alcance

- No agregar andamiaje que todavía no tiene nada que hacer. Linters, suites de
  pruebas y paquetes entran cuando exista el código que justifican, no antes.

## Ramas

- `feat/*` y `fix/*` para lo que se edita en local (`docs/`, `scripts/`, workflows).
- `dev` está sincronizada por git integration con el workspace `ws-gansito-dev`.
  Los notebooks y pipelines se editan en el UI de Fabric y se commitean desde ahí.
- `main` está protegida. Sólo entra por PR desde `dev`. Al mergear, se despliega a
  `ws-gansito-prod` con `fabric-cicd`. **A prod nunca se le edita a mano.**

## Nombres

| prefijo | item |
|---|---|
| `lh_` | lakehouse |
| `nb_NN_` | notebook; `NN` marca la capa (10 bronze, 20 silver, 30 gold, 40 dq, 50 export) |
| `pl_` | data pipeline |
| `sm_` / `rpt_` | modelo semántico / reporte |
| `_col` | columna de metadato técnico, nunca de negocio |

Capas en inglés (`bronze`/`silver`/`gold`); tablas y columnas en español, como la fuente.

## Reglas de arquitectura que no se negocian

1. **Los notebooks no usan lakehouse por defecto.** Resuelven la ruta leyendo su
   workspace en tiempo de ejecución, para que el mismo código corra igual en dev y
   en prod sin reasignar nada.
2. **Bronze no castea, no filtra, no deduplica.**
3. **Nada llega a gold sin pasar por las reglas de calidad.** La observación que falla
   no entra a la medida y tampoco se tira: se cuenta en el hecho y su fila original se
   queda en bronze (decisión #15).
4. **`unpublish_orphans` se queda apagado** en el despliegue: borraría cualquier
   `Report` o `SemanticModel` creado a mano en prod. No es una protección contra
   perder un lakehouse; de eso se encarga el alcance del despliegue.
5. **Sin wheels ni Environments de Fabric.** Los helpers se comparten con
   `%run nb_00_config`.
