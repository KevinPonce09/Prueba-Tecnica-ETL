import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when

def setup_logging():
    logging.basicConfig(
        format='%(asctime)s - %(levelname)s - %(message)s',
        level=logging.INFO
    )
    return logging.getLogger(__name__)

def create_spark_session():
    """Crea una sesión de Spark."""
    return SparkSession.builder.appName("ETL_Spark").getOrCreate()

def extract_data(spark, file_path):
    """Carga los datos desde un archivo Excel en un DataFrame de Spark."""
    try:
        df = spark.read.format("com.crealytics.spark.excel") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load(file_path)
        logging.info("Datos extraídos correctamente")
        return df
    except Exception as e:
        logging.error(f"Error en la extracción: {e}")
        return None

def transform_data(df):
    """Realiza limpieza de datos eliminando nulos y duplicados."""
    try:
        df_clean = df.dropDuplicates().na.drop()
        logging.info("Transformación aplicada: Eliminados duplicados y nulos")
        return df_clean
    except Exception as e:
        logging.error(f"Error en la transformación: {e}")
        return None

def quality_checks(df):
    """Verifica la calidad de los datos."""
    try:
        null_counts = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns])
        null_counts.show()
        logging.info("Chequeo de calidad completado")
    except Exception as e:
        logging.error(f"Error en el chequeo de calidad: {e}")

def load_data(df, output_path):
    """Guarda los datos procesados en formato CSV."""
    try:
        df.write.mode("overwrite").csv(output_path, header=True)
        logging.info("Datos cargados en formato CSV")
    except Exception as e:
        logging.error(f"Error en la carga: {e}")

def main():
    logger = setup_logging()
    spark = create_spark_session()
    
    file_path = "Films2.xlsx"  # Ajusta la ruta si es necesario
    output_path = "output/films_processed"
    
    df = extract_data(spark, file_path)
    if df is not None:
        quality_checks(df)
        df_transformed = transform_data(df)
        if df_transformed is not None:
            load_data(df_transformed, output_path)

if __name__ == "__main__":
    main()
