import logging
from datetime import date
import os
import requests
import pandas

class Categoria:
    """Esta es una clase que representa una categoria
        Atributos:
            tipo (str): Es el nombre de la categoria
            url (str) : Es la url del archivo csv a descargar
            file_path (str) : Es la ruta local del archivo .csv descargado
    """

    def __init__(self, categoria, url) -> None:
        self.categoria = categoria
        self.url = url

    def descargarArchivo(self):
        """Esta función realiza la creacion del directorio y el archivo con nombres formateados,
        también realiza la descarga del archivo .csv en directorio local y
        asigna la ruta al atributo file_path del objeto 
        """
        # Se obtienen la fecha actual
        dia = date.today().day
        mes = date.today().month
        mes_str = date.today().strftime("%B")
        anio = date.today().year
        fecha = f"{dia}-{mes}-{anio}"
        # Se asigna el formato del nombre deldirectorio para la descarga y se crea 
        path_dir = f"./{self.categoria}/{anio}-{mes_str}"
        os.makedirs(path_dir, exist_ok=True)
        # Se asigna el formato del nombre del archivo y se crea el archivo
        path_file = f"{path_dir}/{self.categoria}-{fecha}.csv"
        output = open(path_file, "wb")
        # Se descargar el archivo csv y se escribe el contenido de este al archivo antes creado 
        response = requests.get(self.url)
        output.write(response.content)
        output.close()
        # Se asigna la ruta del archivo al atributo file_path del objeto
        self.file_path = path_file
        logging.info(f"Se creo el directorio y se descargo el archivo localmente de {self.categoria}")

