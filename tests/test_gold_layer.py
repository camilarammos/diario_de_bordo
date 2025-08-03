from src.gold_layer import run_gold

def test_gold_aggregations(spark, tmp_path):
    df = spark.createDataFrame([
        ("2016-01-01 00:10", "Negócio", "Reunião", 2.0),
        ("2016-01-01 01:00", "Pessoal", "Viagem", 1.0),
    ], ["DATA_INICIO", "CATEGORIA", "PROPOSITO", "DISTANCIA"])

    df = df.withColumn("DATA_INICIO", df["DATA_INICIO"].cast("timestamp"))

    input_path = tmp_path / "gold_input"
    output_path = tmp_path / "gold_output"
    df.write.mode("overwrite").parquet(str(input_path))

    run_gold(str(input_path), str(output_path), spark)
    df_gold = spark.read.parquet(str(output_path))

    assert df_gold.count() == 1
    row = df_gold.first()
    assert row.QT_CORR == 2
    assert round(row.VL_AVG_DIST, 2) == 1.5

