"""
Ingestión de datos - Reporte de clusteres
-----------------------------------------------------------------------------------------

Construya un dataframe de Pandas a partir del archivo 'clusters_report.txt', teniendo en
cuenta que los nombres de las columnas deben ser en minusculas, reemplazando los espacios
por guiones bajos; y que las palabras clave deben estar separadas por coma y con un solo 
espacio entre palabra y palabra.


"""
import pandas as pd


def ingest_data():

    #
    # Inserte su código aquí
    #
    rangos_columnas = [(0, 9), (9, 25), (25, 41), (41, 118)]
    nombres_columnas = pd.read_fwf('clusters_report.txt',
                                    nrows=2,
                                    header=None,
                                    colspecs=rangos_columnas).fillna('').apply(lambda x: ' '.join(x).strip().replace(' ', '_').lower()).values
    df = pd.read_fwf('clusters_report.txt',
                      skiprows=4, 
                      names=nombres_columnas
                      )

    df['cluster'] = df['cluster'].ffill()
    df['principales_palabras_clave'] = df[['cluster','principales_palabras_clave']].groupby(['cluster'])['principales_palabras_clave'].transform(lambda linea_datos: ' '.join(linea_datos).replace('.',''))
    df = df.dropna()
    df['porcentaje_de_palabras_clave'] = df['porcentaje_de_palabras_clave'].transform(lambda porcentaje: porcentaje.replace(' %','').replace(',','.').strip())
    df = df.astype({"cluster": 'Int16', "cantidad_de_palabras_clave": 'Int16', 'porcentaje_de_palabras_clave': 'Float64', 'principales_palabras_clave':'string'})
    df = df.replace(r'\s+', ' ', regex=True)
    return df