# Client

Repo que simula un cliente que usa la plataforma de analisis distribuido Coffee Shop Analysis. Hemos desarrollado este repo para poder facilmente testear la implementacion de la plataforma. Tenemos dos metodos estandard que usamos para testear: probar un dataset reducido (TxItems y Txs 202401 y 202501) o probar con el dataset entero. Para poder hacer esto creamos un makefile que nos permite crear tantos clientes como queramos, y con el dataset que queramos.

## Ejecuciones utiles para probar

La forma mas rapida de ejecutar y validar los resultados es usando `make run ...`, esta va a poner a ejecutar un cliente y ademas cuando este termine va a hacer una comparacion de resultados con unos resultados predefinidos que ya sabemos que son correctos.

Para ejecutar cliente 1 con el dataset grande:

```bash
make run CLIENT_NUM=1
```

Para ejecutar cliente 1 con el dataset reducido:

```bash
make run-short CLIENT_NUM=1
```

**Nota sobre Q4**: Q4 siempre devuelve un Top 3 diferente debido al ordenamiento no determinístico de usuarios con la misma cantidad de compras. Por esto, la validación de Q4 verifica que el resultado sea un Top 3 válido en lugar de compararlo línea por línea con un archivo de referencia.

## Otros comandos utiles

```bash
# Limpiar resultados existentes de la carpeta /output
make clean

# Ejecución básica (sin validación automática)
make docker-compose-up CLIENT_NUM=1
make docker-compose-up-short CLIENT_NUM=1

# Detener cliente
make docker-compose-down CLIENT_NUM=1

# Ver ayuda completa
make help
```
