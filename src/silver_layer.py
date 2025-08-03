from pyspark.sql.functions import (
    col, to_timestamp, date_format, trim, lower, udf, regexp_replace, when
)
from pyspark.sql.types import StringType
import logging
import unicodedata

from src.utils import persist_to_mysql

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

def normalize_text_udf(s):
    if s is None:
        return None
    s = unicodedata.normalize('NFKD', s)
    s = s.encode('ASCII', 'ignore').decode('utf-8')
    return s.lower().strip()

normalize_text = udf(normalize_text_udf, StringType())

def run_silver(input_path, output_path, spark):
    logger.info("Iniciando camada Silver.")

    # Padroniza hora de 1 dígito para 2 dígitos (ex: 0:41 -> 00:41)
    df = spark.read.parquet(input_path)
    df = df.withColumn(
        "DATA_INICIO",
        regexp_replace(col("DATA_INICIO"), r" (\d{1}):", r" 0\1:")
    ).withColumn(
        "DATA_FIM",
        regexp_replace(col("DATA_FIM"), r" (\d{1}):", r" 0\1:")
    )

    df_silver = (
        df
        .withColumn("DATA_INICIO", to_timestamp(col("DATA_INICIO"), "MM-dd-yyyy HH:mm"))
        .withColumn("DATA_FIM", to_timestamp(col("DATA_FIM"), "MM-dd-yyyy HH:mm"))
        .withColumn("DT_REFE", date_format(col("DATA_INICIO"), "yyyy-MM-dd"))
        .withColumn("DISTANCIA", col("DISTANCIA").cast("double"))
        .withColumn("CATEGORIA", trim(lower(col("CATEGORIA"))))
        .withColumn("PROPOSITO", trim(lower(col("PROPOSITO"))))
        .withColumn("PROPOSITO", when(col("PROPOSITO").isNull(), "indefinido").otherwise(col("PROPOSITO")))
        .withColumn("CATEGORIA", normalize_text(col("CATEGORIA")))
        .withColumn("PROPOSITO", normalize_text(col("PROPOSITO")))
    )
    
    #df.filter(col("DT_REFE")=='2016-08-17').show(truncate=False)

    logger.info("Preview da camada silver:")
    df_silver.show(truncate=False)
    logger.info(f"Salvando camada Silver em {output_path}")
    df_silver.write.mode("overwrite").parquet(output_path)
    logger.info("Persistindo dados no MySQL")
    persist_to_mysql(df_silver, "silver_info_transportes")
    logger.info("Camada Silver finalizada com sucesso.")

