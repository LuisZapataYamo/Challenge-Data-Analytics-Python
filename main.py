import logging
from decouple import config
from create_tables import DataBase

from ejecucion import normalizacion_datos, Fuente, procesamiento_cines, generacion_tablas_categoria, generacion_tablas_fuentes, generacion_tablas_provincia_categoria

# Inicializamos la configuracion de los logs
logging.basicConfig(filename="logs.log", level=logging.DEBUG, format='%(asctime)s:%(levelname)s:%(message)s')

# Incializamos las constantes con las variables de entorno
URL_MUSEOS = config('URL_MUSEOS', cast=str)
URL_SALAS = config('URL_SALAS', cast=str)
URL_BIBLIOTECAS = config('URL_BIBLIOTECAS', cast=str)


def main():
    """Funcion principal que incializa los objetos Fuente, llama a la funcion para normalizar la informacion de las fuentes, llama a la funcion
        para procesar los datos de la fuente salas_cine, y a las generadoras de tablas de registros por fuente, por categoria y por provincia y categoria 
    """
    # Se asignan las headers a obtener del dataframe
    columns_museos = ['Cod_Loc',
                      'IdProvincia',
                      'IdDepartamento',
                      'categoria',
                      'provincia',
                      'localidad',
                      'nombre',
                      'direccion',
                      'CP',
                      'telefono',
                      'Mail',
                      'Web']
    columns_bibliotecas = ['Cod_Loc',
                           'IdProvincia',
                           'IdDepartamento',
                           'Categoría',
                           'Provincia',
                           'Localidad',
                           'Nombre',
                           'Domicilio',
                           'CP',
                           'Teléfono',
                           'Mail',
                           'Web']
    columns_salas_cine = ['Cod_Loc',
                     'IdProvincia',
                     'IdDepartamento',
                     'Categoría',
                     'Provincia',
                     'Localidad',
                     'Nombre',
                     'Dirección',
                     'CP',
                     'Teléfono',
                     'Mail',
                     'Web']

    columns_salas_cine_prov = [ 'Provincia',
                                'Pantallas',
                                'Butacas',
                                'espacio_INCAA']

    # Creacion de las instancias por fuente
    museos = Fuente("museos", URL_MUSEOS)
    bibliotecas = Fuente("bibliotecas", URL_BIBLIOTECAS)
    salas_cine = Fuente("salas_cine", URL_SALAS)
    
    # Tupla de instancias de las fuentes
    list_fuentes = (museos, bibliotecas, salas_cine)

    # Obtencion de los dataframes de las fuentes
    df_museos = museos.get_dataframe(columns_museos)
    df_bibliotecas = bibliotecas.get_dataframe(columns_bibliotecas)
    df_salas_cine = salas_cine.get_dataframe(columns_salas_cine)

    # Tupla de DataFrames de las fuentes
    list_dfFuentes = (df_museos, df_bibliotecas, df_salas_cine)

    #Actualizando datos de la tabla cines_provincia
    df_salas_cine_provincia = salas_cine.get_dataframe(columns_salas_cine_prov)
    procesamiento_cines(df_salas_cine_provincia)
    
    df_registros = normalizacion_datos(list_dfFuentes)
    
    # Generacion de tablas
    
    # Generacion de tabla de registros por categoria
    generacion_tablas_categoria(df_registros)

    # Generacion de tabla de registros por fuente
    generacion_tablas_fuentes(list_fuentes)

    # Genracion de tabla de registros totales por provincia y categoria
    generacion_tablas_provincia_categoria(df_registros)
    
if __name__ == "__main__":
    main()