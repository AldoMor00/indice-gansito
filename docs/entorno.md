# El entorno: lo que vive fuera de git

Todo lo que hay que configurar a mano para que el proyecto corra, en el orden en que se
reconstruye. El despliegue no lo lleva y el clon del repo tampoco. La decisión #1 da por normal
borrar un workspace y volverlo a armar; esta es la lista para hacerlo.

Cada punto dice qué es, dónde se prende y qué pasa si falta.

## 1. La capacidad y los dos workspaces

`ws-gansito-dev` y `ws-gansito-prod`, los dos sobre la misma capacidad, que es una trial. Eso
tiene dos consecuencias que el resto de la lista arrastra: la capacidad no aguanta dos sesiones
de Spark a la vez (`plataforma.md`), y cuando expire los items de Fabric se borran a los siete
días (decisión #6). Lo que deba sobrevivir tiene que estar en git antes.

## 2. Los lakehouses de prod

`lh_bronze`, `lh_silver` y `lh_gold` se crean a mano en prod, **con esquemas habilitados**. El
despliegue los excluye a propósito: son los dueños de los datos y el CI no los administra
(decisión #4). En dev los crea la git integration al sincronizar.

Si faltan, el primer notebook truena al resolver la ruta por nombre.

## 3. Spark settings, en los dos workspaces

Workspace settings > Data Engineering/Science > Spark settings.

- **Runtime version: 2.0**, la misma en dev y en prod. Las tablas nacen en un protocolo de Delta
  que un runtime más viejo no lee, y dev clona el bronze de prod (decisión #11). Se suben los dos
  o ninguno.
- **High concurrency > For pipeline running multiple notebooks: encendido.** Es lo que deja que
  las actividades de un pipeline compartan sesión; el `sessionTag` solo agrupa (decisión #10).
  Apagado, la segunda actividad pide sesión propia y se topa con el 430 de la capacidad. El
  pipeline sale rojo, no verde con datos malos.

El perfil de recursos del workspace no importa: cada notebook declara el suyo (decisión #33).

## 4. Git integration, sólo en dev

`ws-gansito-dev` conectado al repositorio `indice-gansito`, rama `dev`, carpeta `fabric/`.
Los notebooks y pipelines se commitean desde el panel de código fuente del UI. Prod no se conecta:
se le despliega desde `main` (decisión #4).

## 5. La identidad del despliegue

Desde cero, porque es lo que más jerga junta.

Un **service principal** es una cuenta para programas, no para personas: un identificador con
permisos, sin contraseña que alguien escriba. Aquí se llama `sp-indice-gansito-deploy` y es una
**app registrada en Microsoft Entra** (el directorio de identidades del tenant). Es la cuenta con
la que GitHub Actions publica los items en prod.

Cómo se autentica sin secreto: la app tiene una **credencial federada** que confía en los tokens
que GitHub emite para este repositorio. El workflow pide uno (`id-token: write` en `deploy.yml`),
se lo presenta a Entra y recibe acceso. No hay client secret guardado en ningún lado, así que no
hay nada que rote ni que se filtre.

Qué permisos necesita, y por qué en los dos workspaces:

- **Contributor en prod**, para publicar los items. Es el mínimo que `fabric-cicd` documenta.
- **Contributor en dev**, porque `nb_91_clona_bronze` corre dentro de `pl_mantenimiento` de prod
  con esta identidad y escribe el bronze de dev (decisión #33). Un notebook dentro de un pipeline
  corre como quien modificó el pipeline al último, y en prod ese es el despliegue (`plataforma.md`).
  Sin el rol, `pl_mantenimiento` sale rojo en su último paso.

Admin no hace falta: agregaría borrar el workspace y administrar accesos, y nada lo usa.

## 6. GitHub

En el repositorio `indice-gansito`:

| qué | nombre | para qué |
|---|---|---|
| variable | `AZURE_CLIENT_ID` | el id de la app de Entra del punto 5 |
| variable | `AZURE_TENANT_ID` | el tenant donde vive |
| variable | `FABRIC_WORKSPACE_ID_PROD` | el GUID de `ws-gansito-prod`, destino del despliegue |
| environment | `prod` | donde corre `deploy.yml`; la credencial federada se acota a él |
| secreto | `DATOS_DEPLOY_KEY` | deploy key con escritura en `indice-gansito-datos`, para que la ingesta commitee ahí sin colgar de una cuenta personal |
| secreto | `INEGI_TOKEN` | el token de la API de INEGI; va en la URL, por eso es secreto y el manifiesto guarda `{token}` |

Y un ruleset que impide borrar `main` y `dev`. No obliga a que `main` entre sólo por PR: `dev` no
puede exigirlo sin romper la git integration (queda como decisión abierta en `PROGRESO.md`).

Los workflows con `schedule` o `workflow_dispatch` sólo se registran desde la rama por defecto:
mientras un workflow viva sólo en `dev`, no existe.

## 7. La conexión de Power BI

La actividad `refresh_sm_gansito` de `pl_gold` y de `pl_mantenimiento` refresca el modelo a través
de una **conexión** de Power BI, creada a mano por un usuario. Cada pipeline la referencia por ID
en `externalReferences.connection`.

- Si se recrea, cambia el ID y hay que volver a elegirla en las dos actividades.
- **Tiene que estar compartida con `sp-indice-gansito-deploy` con rol User** (Manage connections
  and gateways > Manage users). Sin eso, el despliegue no publica ninguno de los dos pipelines, y
  arrastra a `pl_master`, que invoca a `pl_gold`.

## 8. Los avisos de falla de los schedules

`pl_master` (martes 03:00, hora de México) y `pl_mantenimiento` (domingo 03:00) llevan su horario
en `.schedules`, que sí viaja en git y que `parameter.yml` enciende sólo en prod. Lo que no viaja
es **a quién avisar cuando fallen**: Home > Schedule > Failure notifications, en cada pipeline.
Es el único aviso de fallo del proyecto (decisión #31).

## 9. El token con que prod publica gold

`nb_50_export` sube los parquets de gold a `indice-gansito-datos` desde prod (decisión #6). No
puede usar la deploy key del punto 6, que vive en GitHub Actions; usa un token guardado en Azure
Key Vault.

- **Key Vault `kv-indice-gansito`**, en el resource group `rg-indice-gansito`, región Mexico
  Central, con permisos por RBAC y **sin *purge protection***: así el vault se puede borrar y
  rearmar como todo lo demás (decisión #1). La red queda pública; un firewall de vault lo dejaría
  fuera del alcance de Fabric.
- **Secreto `github-indice-gansito-datos`**: un token *fine-grained* de GitHub, con acceso sólo a
  `indice-gansito-datos` y el permiso *Contents: Read and write*. Nada más.
- **Tres roles sobre el vault**, porque son tres identidades y ninguna hereda de las otras. Ser
  Owner de la suscripción no da acceso al plano de datos: el rol se asigna aparte.

| identidad | rol | para qué |
|---|---|---|
| `sp-indice-gansito-deploy` | Key Vault Secrets User | corre los notebooks de prod dentro de un pipeline (punto 5) |
| la cuenta de Fabric del tenant | Key Vault Secrets User | corre `nb_50_export` a mano |
| la cuenta dueña de la suscripción de Azure | Key Vault Secrets Officer | crea y **rota** el secreto |

Las dos cuentas de persona son distintas y tienen el mismo nombre para mostrar, así que en el
selector del portal salen como dos renglones idénticos: se eligen por UPN. *Secrets User* no
alcanza para escribir el secreto, por eso la que rota lleva *Officer*.

El token caduca. Cuando pasa, `nb_50_export` truena en prod con 401 y `pl_gold` sale rojo, aunque
gold ya quedó escrito y el modelo refrescado. Se genera otro token, se reemplaza el valor del
secreto y se corre `nb_50_export` solo, sin tocar código.

Si el vault o el secreto cambian de nombre, cambian `BOVEDA` y `SECRETO` en `nb_50_export`.

## 10. Las alertas de costo

Un presupuesto de Cost Management, `presupuesto-kv-indice-gansito`, sobre el resource group
`rg-indice-gansito`: 1 USD al mes, con aviso por correo al 10 % y al 100 % del gasto real
(decisión #44). Corre hasta el 31 de diciembre de 2030; ese día deja de evaluar y no lo avisa.

Un presupuesto **avisa, no detiene el gasto**: evalúa sobre costo ya consolidado y el correo sale
dentro de la hora siguiente a la evaluación. Lo que va a controlar lo que cuesta la capacidad es
pausarla.

**Fuera de ese resource group no hay alerta de costo.** Si se quiere una, la cuenta de Azure es un
Microsoft Customer Agreement y un presupuesto de alcance mayor cuelga de la **cuenta de
facturación**, no de la suscripción: `az consumption budget list` consulta el alcance de la
suscripción y no lo vería.

## 11. Lo que todavía no existe

La cuenta pública y el clon import de la decisión #6. Cuando se arme, va aquí: qué cuenta, qué
tenant y el setting de *Publish to web*.
