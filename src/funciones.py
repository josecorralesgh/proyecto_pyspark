##Entorno de funciones

##función para guardar el dato en disco formato csv
def guardar_dataframe_csv(dataframe, ruta_guardado, nombre_archivo, modo='overwrite'):
    """
    Guarda un DataFrame en formato CSV.

    Parámetros:
    - dataframe: DataFrame de PySpark a guardar.
    - ruta_guardado: Ruta donde se guardará el archivo CSV.
    - nombre_archivo: Nombre del archivo CSV.
    - modo: Modo de guardado ('overwrite', 'append', 'ignore', 'error'). Por defecto, 'overwrite'.
    """
    # Ruta completa del archivo CSV
    ruta_completa = f"{ruta_guardado}/{nombre_archivo}.csv"

    # Guardar el DataFrame en formato CSV
    dataframe.write.csv(ruta_completa, header=True, mode=modo)
    #result_df.write.csv("ruta/del/archivo/resultados.csv", header=True, mode='overwrite')

##funicón para completar columnas, con valor constante
def data_filled(dataframe,colname,valor):
    from pyspark.sql.functions import  when
    data_filled = dataframe.withColumn(colname, when(dataframe[colname].isNull(), valor).otherwise(dataframe[colname]))
    # Devolver el nuevo DataFrame
    return data_filled

