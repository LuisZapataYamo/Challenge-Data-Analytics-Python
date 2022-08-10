# Challenge Data Analytics + Python
- Realización del reto del Chellenge de Alkemy

## Creación y activación del entorno virtual
- Primero instalamos la libreria virtualenv
    ```
    pip install virtualenv
    ```
- Creamos el entorno virtual
    ```
    python -m virtualenv venv
    ```
- Activamos el entorno virtual
    - En bash 
    ```
        source ./venv/Scripts/activate
    ```
    - En cmd
    ```
        ./venv/Scripts/activate.bat
    ```

## Instalación de librerias
- Primero se debe instalar los paquetes 
    ```
    pip install -r requirements.txt
    ```
## Configuracion de Base de Datos
- Creamos un archivo .env en la raiz del proyecto y colocamos los valores para las variables de entorno de la base de datos y de las URL de las fuentes.
    - URL_MUSEOS: Es la url de la fuente museos
    - URL_SALAS: Es la url de la fuente salas de cine
    - URL_BIBLIOTECAS: Es la url de la fuente bibliotecas
    - USER_DB: Es el usuario de la base de datos
    - PASSWORD_DB: Es el password del usuario de la base de datos
    - NAME_DB: Es el nombre de la base de datos
    - HOST_DB: Es el nombre del host de la base de datos
    - PORT_DB: Es el puerto de la base de datosw
    ```
    URL_MUSEOS=
    URL_SALAS=
    URL_BIBLIOTECAS=
    USER_DB=
    PASSWORD_DB=
    NAME_DB=
    HOST_DB=
    PORT_DB=
    ```