
from src.silver_layer import run_silver

def test_silver_transformation(spark, tmp_path):
    df = spark.createDataFrame([
        ("01-01-2016 0:41", "01-01-2016 1:10", "Negócio", "A", "B", "Reunião", 5.0)
    ], ["DATA_INICIO", "DATA_FIM", "CATEGORIA", "LOCAL_INICIO", "LOCAL_FIM", "PROPOSITO", "DISTANCIA"])

    input_path = tmp_path / "silver_input"
    output_path = tmp_path / "silver_output"
    df.write.mode("overwrite").parquet(str(input_path))

    run_silver(str(input_path), str(output_path), spark)
    df_out = spark.read.parquet(str(output_path))

    assert "DT_REFE" in df_out.columns
    assert df_out.count() == 1
    
    # Verifica se o valor da coluna DT_REFE está correto
    row = df_out.first()
    assert row is not None
    assert row["DT_REFE"] == "2016-01-01"
