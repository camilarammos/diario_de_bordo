from pyspark.sql import SparkSession
import logging

from src.utils import persist_to_mysql

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

def run_raw(input_path, output_path, spark):
    logger.info("Iniciando camada Raw.")
    logger.info(f"Lendo arquivo CSV de entrada: {input_path}")
    df = spark.read.csv(input_path, header=True, sep=';')
    logger.info(f"Primeiras linhas do arquivo lido:")
    df.show(3)
    logger.info(f"Salvando como Parquet em: {output_path}")
    df.write.mode("overwrite").parquet(output_path)
    logger.info("Persistindo dados no MySQL")
    persist_to_mysql(df, "raw_info_transportes")
    logger.info("Camada Raw finalizada com sucesso.")

