import pandas as pd
from datetime import date
import requests
import logging
import os

from create_tables import DataBase

class Categoria:
    """Esta es una clase que representa una categoria
        Atributos:
            name (str) : Es el nombre de la categoria
            url (str) : Es la url del archivo csv a descargar
            file_path (str) : Es la ruta local del archivo .csv descargado
            columns_extract (list(str)) : Lista nombres de columnas a extraer del csv
        
    """
    

    def __init__(self, name, url, columns, file_path = ""):
        """Este constructor inicializa los atributos
            Parametros:
                name (str) : Es el nombre de la categoria
                url (str) : Es la url para descargar el archivo csv 
        """
        self.name = name
        self.url = url
        self.columns_extract = columns
        self.file_path = file_path
        self.descargarArchivos()

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


    def descargarArchivos(self):
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
        # Se asigna el formato del nombre del archivo y se crea el archivo
        file_path = f"{path_dir}/{self.get_name()}-{fecha}.csv"
        output = open(file_path, "wb")
        # Se descargar el archivo csv y se escribe el contenido de este al archivo antes creado 
        response = requests.get(self.get_url())
        output.write(response.content)
        output.close()
        # Se asigna la ruta del archivo al atributo file_path del objeto
        self.file_path = file_path
        logging.info(f"Se creo el directorio y se descargo el archivo localmente de {self.name}")

    def get_dataframe(self, columns = ()):
        # Se crea el dataframe de la categoria
        df = pd.read_csv(self.file_path)
        return df if len(columns) == 0 else df[columns]
        

def normalizacionDatos(df):
        # Se renombran los headers para la creacion de una unica tabla
        df.columns = ['cod_localidad',
                            'id_provincia',
                            'id_departamento',
                            'categoría',
                            'provincia',
                            'localidad',
                            'nombre',
                            'domicilio',
                            'código postal',
                            'número de teléfono',
                            'mail',
                            'web']
        return df

def actualizacionDatos(table,df: pd.DataFrame, conn):
    fecha_actual = date.today().strftime("%Y/%m/%d")
    df["fecha_carga"] = []
    df = df["fecha_carga"].map(f"{fecha_actual}") 
    df.to_sql(table, conn, if_exists="append", index=True, index_label="id")
