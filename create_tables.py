import logging
from decouple import config
import sqlalchemy as db

# Configuracion general del Logging
logging.basicConfig(filename="logs.log", level=logging.DEBUG, format='%(asctime)s : %(levelname)s : %(message)s')

class DataBase:
    USER_DB = config("USER_DB", cast=str)
    PASSWORD_DB = config("PASSWORD_DB", cast=str)
    NAME_DB = config("NAME_DB", cast=str)
    HOST_DB = config("HOST_DB", cast=str)
    PORT_DB = config("PORT_DB", cast=str)
    engine = db.create_engine(
        f'postgresql://{USER_DB}:{PASSWORD_DB}@{HOST_DB}:{PORT_DB}/{NAME_DB}')

    def __init__(self):
        try:
            self.conexion = DataBase.engine.connect()
            logging.info(f"Conexion a la base de datos exitosa")
        except Exception:
            logging.exception("Error en la conexion con la base de datos")

    def get_conexion(self):
        return self.conexion

def create_tables(file_path):
    base_datos = DataBase()
    try:
        with open(file_path, 'r') as file:
            sql = file.read()
            base_datos.get_conexion().execute(sql)
    except Exception:
        logging.exception(f"Error al leer el script {file_path}")

if __name__ == "__main__":
    create_tables("./models/models.sql")