# Decisiones

Qué se decidió, qué se descartó y por qué. Donde algo no se haría así en producción,
se dice.

## 1. La zona raw vive fuera de Fabric

El histórico se guarda en `indice-gansito-datos`, no en OneLake, porque la capacidad es
una trial y va a desaparecer con sus datos. Con el raw afuera, Fabric queda desechable:
se borra el workspace y se reconstruye sin perder historia. En producción viviría en
ADLS; git no es un almacén de datos.

## 2. Se filtra en la puerta

De cada CSV de ~155 MB se persisten dos cortes: las filas del catálogo objetivo, y las
tuplas distintas de tienda —estas del archivo **completo**, para que `dim_tienda` no
quede sesgada a las tiendas que venden pastelillos.

Rompe la inmutabilidad del raw y es la concesión más grande del proyecto. Se mitiga
guardando el `sha256` y la URL de origen en el manifiesto, para poder rehacer cualquier
corte desde la fuente.

## 3. Los notebooks no usan lakehouse por defecto

El enlace del UI guarda el GUID del lakehouse: al desplegar a otro workspace sigue
apuntando al origen y el notebook corre en verde sobre los datos equivocados. Los
notebooks leen su workspace en tiempo de ejecución y arman la ruta con nombres. Lo que
sí hay que reasignar —pipelines, modelo semántico, reporte, conexiones— vive en
`fabric/parameter.yml`.

## 4. Prod no está conectado a git

Sólo `ws-gansito-dev` tiene git integration. A prod se le despliega desde `main` con
`fabric-cicd` y credencial federada OIDC, sin secretos guardados. Rollback = revertir el
commit. `unpublish_all_orphan_items` no se llama: puede borrar un lakehouse con datos.

## 5. Dos ambientes, sin test

Dev no ingesta: lee el `lh_bronze` de prod por un shortcut de sólo lectura y escribe en
sus propios silver y gold. Así desarrolla contra datos reales, que es justo lo que un
ambiente de test iría a comprobar.

## 6. Dos modelos semánticos

Direct Lake exige capacidad y no se puede mover a una cuenta gratuita. El de Fabric es
Direct Lake sobre `lh_gold`; el público es un PBIX en modo import que lee los agregados
exportados a CSV por URL anónima. El costo es que el DAX vive duplicado.

## 7. Nada de wheels: `%run` y pruebas en notebook

Publicar una wheel a un Environment de Fabric toma minutos y mata la iteración. Los
notebooks comparten helpers con `%run nb_00_config`; las pruebas de código van en
`nb_90_pruebas` y las de datos en `nb_40_dq`, que escribe su veredicto en
`dq_resultados` en vez de tronar con un assert suelto.

El Python que corre en GitHub Actions es caso aparte: nunca entra a Fabric, así que ahí
sí hay `pytest` y `ruff` normales.
