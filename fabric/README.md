# Carpeta de items de Fabric

**Esta carpeta la escribe Fabric, no una persona.** `ws-gansito-dev` esta conectado por
git integration a la rama `dev` apuntando aqui. Editar estos archivos a mano rompe la
sincronizacion.

La unica excepcion es `parameter.yml`, que traduce al desplegar a prod lo que la cadena
no resuelve sola. Hoy esta vacio, y se midio por que: la git integration guarda las
referencias entre items como `logicalId` y el workspace como GUID nulo, y `fabric-cicd`
los resuelve al publicar. Los notebooks ni eso necesitan: arman su ruta en tiempo de
ejecucion. Ver `docs/hechos.md`.

## Lo que el workspace necesita configurado

Esto vive fuera de git, asi que el despliegue no lo lleva. Al reconstruir un workspace
desde cero —que la decision #1 da por normal— hay que volver a prenderlo a mano, en dev
y en prod:

- **Spark settings > High concurrency > For pipeline running multiple notebooks.**
  Es lo que habilita que el `sessionTag` de `pl_bronze` agrupe; el tag solo no basta. Sin
  esto las actividades corren en sesiones separadas y la segunda se topa con 430. No
  miente en verde: el pipeline sale rojo. Ver decision #10.
