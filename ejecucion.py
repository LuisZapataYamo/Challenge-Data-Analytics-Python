import pandas as pd
from datetime import date
from sqlalchemy import Integer, String, Date
import requests
import logging
import os

from create_tables import DataBase

class Fuente:
    """Esta es una clase que representa una categoria
        Atributos:
            name (str) : Es el nombre de la categoria
            url (str) : Es la url del archivo csv a descargar
            file_path (str) : Es la ruta local del archivo .csv descargado
            columns_extract (list(str)) : Lista nombres de columnas a extraer del csv
        
    """
    

    def __init__(self, name, url, file_path = ""):
        """Este constructor inicializa los atributos
            Parametros:
                name (str) : Es el nombre de la categoria
                url (str) : Es la url para descargar el archivo csv 
        """
        self.name = name
        self.url = url
        self.file_path = file_path
        self.descargar_csv()

    def get_name(self):
        """Esta funcion retorna el atributo name de la instancia
            Return:
                str
        """
        return self.name

    def get_file_path(self):
        """Esta funcion retorna el atributo file_path de la instancia
            Return:
                str
        """
        return self.file_path
    
    def set_file_path(self, file_path):
        """Esta funcion retorna el atributo file_path de la instancia
            Return:
                str
        """
        self.file_path = file_path


    def descargar_csv(self):
        """Esta función realiza la creacion del directorio y el archivo con nombres formateados,
            también realiza la descarga del archivo .csv en directorio local y
            asigna la ruta al atributo file_path del objeto 
            """
        # Se obtienen la fecha actual
        fecha_actual = date.today()
        mes_str = ("enero", "febrero", "marzo", "abril", "mayo", "junio", "julio", "agosto", "septiembre", "octubre", "noviembre", "dicimebre")
        fecha = fecha_actual.strftime("%d-%m-%Y")

        # Se asigna el formato del nombre deldirectorio para la descarga y se crea 
        path_dir = f"./{self.get_name()}/{fecha_actual.day}-{mes_str[fecha_actual.month-1]}"
        os.makedirs(path_dir, exist_ok=True)
        logging.info(f"Se creo el directorio {path_dir} para el archivo {self.name}")

        # Se asigna el formato del nombre del archivo y se crea el archivo
        file_path = f"{path_dir}/{self.get_name()}-{fecha}.csv"
        output = open(file_path, "wb")

        try:
            # Se descargar el archivo csv y se escribe el contenido de este al archivo antes creado 
            response = requests.get(self.url)
            output.write(response.content)
            output.close()
        except Exception:
            logging.exception(f"Error en la descarga del archivo de la url {self.url}")
        
        # Se asigna la ruta del archivo al atributo file_path del objeto
        self.file_path = file_path
        logging.info(f"Se descargo el archivo de {self.name} y esta ubicado en {self.name}")

    def get_dataframe(self, columns = ()):
        """Esta función realiza la lectura del csv con pandas y devuelve un dataframe
            Return:
                pandas.DataFrame
        """
        try:
            # Se crea el dataframe de la categoria
            df = pd.read_csv(self.file_path)
            logging.info(f"Se obtuvo correctamente el dataframe {self.name}")
            return df if len(columns) == 0 else df[columns]
        except Exception:
            logging.exception(f"Error al obtener dataframe de {self.name} con las columnas ingresadas {columns}")
        
def normalizacion_datos(dfs):
    """Esta función cambia los header de los dataframes y concatena los dataframes uno solo, inicializa un diccionario con los tipos de datos de las columna para la tablea a actualizar, se actualiza los datos
        mediante la función actualizacion_datos.
        Params:
            dfs: list(pandas.DataFrame)
        Return:
            pandas.DataFrame
    """
    dtype={
                'cod_localidad': Integer(),
                'id_provincia': Integer(),
                'id_departamento': Integer(),
                'categoria': String(),
                'provincia': String(),
                'localidad': String(),
                'nombre': String(),
                'domicilio': String(),
                'codigo postal': String(),
                'numero de telefono': String(),
                'mail': String(),
                'web': String()}
    # Se renombran los headers para la creacion de una unica tabla
    lista_df = []
    for dfn in dfs:
        # Se renombran los header en el dataframe
        dfn.columns = list(dtype.keys())
        lista_df.append(dfn)
    df_registros = pd.concat(lista_df)
    
    # Se llama a a la funcion para actualizar los registro en la tabla registros de la BD
    actualizacion_datos(table="registros",df=df_registros, dtype=dtype)
    return df_registros

def procesamiento_cines(df_salas_cine: pd.DataFrame):
    """Esta función inicializa un diccionario con los tipo de datos para los campos de la tabla salas_provincia,
        se actualiza los datos mediante la función actualizacion_datos.
        Params:
            df_salas_cine: pandas.DataFrame    
    """

    df_salas_cine.columns = ["provincia", "pantallas", "butacas", "espacio_INCAA"]

    set_provincias = set(df_salas_cine["provincia"].to_list())
    lista_dfresults =  []
    try:
        for provincia in set_provincias:
            df_aux = df_salas_cine[df_salas_cine["provincia"] == provincia]
            pantallas = df_aux["pantallas"].sum()
            butacas = df_aux["butacas"].sum()
            espacios_INCAA = df_aux[df_aux["espacio_INCAA"] != 0]["espacio_INCAA"].count()
            nueva_fila = {"provincia": [provincia], "pantallas": [pantallas], "butacas": [butacas], "espacios_INCAA": [espacios_INCAA]}
            df_result = pd.DataFrame(nueva_fila)
            lista_dfresults.append(df_result)
        df_result = pd.concat(lista_dfresults)
        dtype = {"provincia": String(), "pantallas": Integer(), "butacas": Integer(), "espacios_INCAA": Integer() ,"fecha_carga": Date()}
    except Exception:
        logging.exception("Error al crear dataframe de salas_provincia")
    actualizacion_datos(table="salas_provincias",df=df_result, dtype=dtype)


def actualizacion_datos(table,df: pd.DataFrame, dtype):
    """Estas función actualiza los registros en la BD, reemplazandos los anteriores,segun el nombre de la tabla con los datos del dataframe y con 
        los nombres de las columnas y sus tipos de datos, también añadiendo un campo con la fecha de carga de los datos.
        Params:
            table: str
            df: pandas.DataFrame
            dtype: dict
    """

    # Conexion a la base de datos para actualizacion y procesamiento
    base_datos = DataBase()
    conn = base_datos.get_conexion()

    #Limpiando las tablas
    conn.execute(f"DELETE FROM {table};")
    fecha_actual = date.today().strftime("%Y/%m/%d")

    # Agregando campo fecha de carga en el dataframe
    df["fecha_carga"] = f"{fecha_actual}"
    df.index = range(1, df.shape[0]+1)

    # Reemplazando valores en la base de datos
    df.to_sql(name=table, con=conn, if_exists="replace", index=True, dtype=dtype, index_label="id")

    # Alterando el tipo de dato de id en la tabla de la bd
    conn.execute(f"ALTER TABLE {table} ALTER COLUMN id TYPE INTEGER;")
    
    # Agregando id como primary key 
    conn.execute(f"ALTER TABLE {table} ADD PRIMARY KEY (id);")

    logging.info(f"La tabla {table} se actualizo correctamente")

def generacion_tablas_categoria(df_registros: pd.DataFrame):
    """Esta función genera una tabla que muestra los registros totales por cada fuente
        Params:
            df_registros: pandas.DataFrame
    """
    result_data = {"categoria": [], "registros_totales": []}
    set_categorias = set(df_registros["categoria"].to_list())
    for categoria in set_categorias:
        result_data["categoria"].append(categoria)
        registros = df_registros[df_registros["categoria"] == categoria].shape[0]
        result_data["registros_totales"].append(registros)
    result_table = pd.DataFrame(result_data)
    print("Registros totales por categoría",result_table, sep="\n")
    logging.info(f"Se genero la tabla de registros por categoria")

def generacion_tablas_fuentes(list_fuentes):
    """Esta función genera una tabla que muestra los registros totales por cada fuente
        Params:
            list_fuentes: list(Fuente)
    """
    result_data = {"fuente": [], "registros_totales": []}
    for fuente in list_fuentes:
        result_data["fuente"].append(fuente.get_name())
        result_data["registros_totales"].append(fuente.get_dataframe().shape[0])
    result_table = pd.DataFrame(result_data)
    print("Registros totales por fuente",result_table, sep="\n")
    logging.info(f"Se genero la tabla de registros por fuente de datos")

def generacion_tablas_provincia_categoria(df_registros: pd.DataFrame):
    """Esta función genera una tabla cruzada que muestra los registros totales segun su provincia y categoria
        Params:
            df_registros: pandas.DataFrame
    """
    set_provincias = set(df_registros["provincia"].to_list())
    set_categorias = set(df_registros["categoria"].to_list())
    result_data = {f"{categoria}": [] for categoria in set_categorias}
    result_data["Provincia"] = []
    for provincia in set_provincias:
        result_data["Provincia"].append(provincia)
        for categoria in set_categorias:
            registros = df_registros[(df_registros["provincia"] == provincia) & (df_registros["categoria"] == categoria)]
            result_data[categoria].append(registros.shape[0])
    result_table = pd.DataFrame(result_data)
    result_table = result_table.set_index("Provincia")
    print("Registros totales por provincia y categoria",result_table, sep="\n")
    logging.info(f"Se genero la tabla de registros por provincia y categoria")