import logging
from pyspark.sql import SparkSession
from src.raw_layer import run_raw
from src.silver_layer import run_silver
from src.gold_layer import run_gold

# Configuração básica do logger
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Caminhos dos arquivos
RAW_CSV = "data/raw/info_transportes.csv"
RAW_PARQUET = "data/raw/info_transportes_raw.parquet"
SILVER_PARQUET = "data/silver/info_transportes_silver.parquet"
GOLD_PARQUET = "data/gold/info_corridas_do_dia.parquet"

def main():
    logger.info("==== INICIANDO SESSÃO SPARK ====")
    spark = SparkSession.builder.appName("PipelineDiarioDeBordo") \
        .config("spark.jars", "/opt/spark/jars/mysql-connector-j-8.3.0.jar") \
        .getOrCreate()

    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

    # Camada RAW
    logger.info("==== RAW LAYER ====")
    try:
        run_raw(RAW_CSV, RAW_PARQUET, spark)
        logger.info("Camada RAW executada com sucesso.")
    except Exception as e:
        logger.exception("Erro na camada RAW: %s", e)

    # Camada SILVER
    logger.info("==== SILVER LAYER ====")
    try:
        run_silver(RAW_PARQUET, SILVER_PARQUET, spark)
        logger.info("Camada SILVER executada com sucesso.")
    except Exception as e:
        logger.exception("Erro na camada SILVER: %s", e)

    # Camada GOLD
    logger.info("==== GOLD LAYER ====")
    try:
        run_gold(SILVER_PARQUET, GOLD_PARQUET, spark)
        logger.info("Camada GOLD executada com sucesso.")
    except Exception as e:
        logger.exception("Erro na camada GOLD: %s", e)

    spark.stop()
    logger.info("Pipeline finalizado.")

if __name__ == "__main__":
    main()

