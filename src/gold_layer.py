from pyspark.sql.functions import col, date_format, count, when, max, min, avg, round
import logging

from src.utils import persist_to_mysql

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

def run_gold(input_path, output_path, spark):
    logger.info("Iniciando camada Gold.")

    df = spark.read.parquet(input_path)

    logger.info("Iniciando agregação por DT_REFE (data de referência).")
    df_gold = (
        df.withColumn("DT_REFE", date_format(col("DATA_INICIO"), "yyyy-MM-dd"))
        .groupBy("DT_REFE")
        .agg(
            count("*").alias("QT_CORR"),
            count(when(col("CATEGORIA") == "negocio", True)).alias("QT_CORR_NEG"),
            count(when(col("CATEGORIA") == "pessoal", True)).alias("QT_CORR_PESS"),
            max("DISTANCIA").alias("VL_MAX_DIST"),
            min("DISTANCIA").alias("VL_MIN_DIST"),
            round(avg("DISTANCIA"), 1).alias("VL_AVG_DIST"),
            count(when(col("PROPOSITO") == "reuniao", True)).alias("QT_CORR_REUNI"),
            count(when((col("PROPOSITO").isNotNull()) & (col("PROPOSITO") != "indefinido") & (col("PROPOSITO") != "reuniao"), True)).alias("QT_CORR_NAO_REUNI"),
        )
    )

    logger.info("Preview da tabela gold:")
    df_gold.show(truncate=False)
    logger.info(f"Salvando agregação gold como Parquet em: {output_path}")
    df_gold.write.mode("overwrite").parquet(output_path)

    csv_output = "data/gold/info_corridas_do_dia.csv"
    logger.info(f"Salvando também como CSV para análise rápida em: {csv_output}")
    df_gold.coalesce(1).write.mode("overwrite").option("header", True).csv(csv_output)
    
    logger.info("Persistindo dados no MySQL")
    persist_to_mysql(df_gold, "info_corridas_do_dia") 
    logger.info("Camada Gold finalizada com sucesso.")

