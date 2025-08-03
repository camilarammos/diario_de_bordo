import time
import logging

logger = logging.getLogger(__name__)

def persist_to_mysql(df, table_name, mode="overwrite", retries=3, delay=5):
    jdbc_url = "jdbc:mysql://db:3306/db_ce"
    user = "ce"
    password = "senha123"
    driver = "com.mysql.cj.jdbc.Driver"

    for attempt in range(1, retries + 1):
        try:
            logger.info(f"Tentando persistir '{table_name}' no MySQL (tentativa {attempt})...")
            df.write \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", table_name) \
                .option("user", user) \
                .option("password", password) \
                .option("driver", driver) \
                .mode(mode) \
                .save()
            logger.info(f"Persistência da tabela '{table_name}' concluída com sucesso.")
            return
        except Exception as e:
            logger.warning(f"[Tentativa {attempt}] Erro ao persistir '{table_name}': {e}")
            if attempt < retries:
                logger.info(f"Aguardando {delay} segundos antes de nova tentativa...")
                time.sleep(delay)
            else:
                logger.error(f"Falha ao persistir '{table_name}' após {retries} tentativas.")
                raise

