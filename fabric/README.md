# Carpeta de items de Fabric

**Esta carpeta la escribe Fabric, no una persona.** `ws-gansito-dev` esta conectado por
git integration a la rama `dev` apuntando aqui. Editar estos archivos a mano rompe la
sincronizacion.

La unica excepcion es `parameter.yml`, que traduce al desplegar a prod lo que la cadena
no resuelve sola. Casi nada lo necesita: la git integration guarda las referencias a
notebooks como `logicalId` y el workspace como GUID nulo, y `fabric-cicd` los resuelve al
publicar. La excepcion es el refresh de `pl_gold`, que guarda el workspace y el modelo de
dev como GUIDs literales. Ver `docs/hechos.md`.

## Lo que el workspace necesita configurado

Esto vive fuera de git, asi que el despliegue no lo lleva. Al reconstruir un workspace
desde cero —que la decision #1 da por normal— hay que volver a prenderlo a mano, en dev
y en prod:

- **Spark settings > High concurrency > For pipeline running multiple notebooks.**
  Es lo que habilita que el `sessionTag` de `pl_bronze` agrupe; el tag solo no basta. Sin
  esto las actividades corren en sesiones separadas y la segunda se topa con 430. No
  miente en verde: el pipeline sale rojo. Ver decision #10.
- **Spark settings > Runtime version > 2.0**, la misma en los dos workspaces. Las tablas
  que crea Delta 4.2 nacen en un protocolo que un Delta 3.x no lee, asi que un workspace
  rezagado no podria clonar el bronze del otro. Ver decision #11.
- **Una conexion de Power BI** para la actividad `refresh_sm_gansito` de `pl_gold`. El
  pipeline la referencia por ID en `externalReferences.connection`; si se recrea, cambia
  el ID y hay que volver a elegirla en la actividad. Con que identidad la usa prod, que
  despliega un service principal, no esta verificado.
