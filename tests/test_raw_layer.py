from src.raw_layer import run_raw

def test_raw_layer_reads_and_writes(tmp_path, spark):
    input_csv = tmp_path / "input.csv"
    output_parquet = tmp_path / "output.parquet"

    input_csv.write_text("DATA_INICIO;DATA_FIM;CATEGORIA;LOCAL_INICIO;LOCAL_FIM;PROPOSITO;DISTANCIA\n"
                         "01-01-2016 00:10;01-01-2016 00:30;Negócio;A;B;Reunião;5.5")

    run_raw(str(input_csv), str(output_parquet), spark)

    df = spark.read.parquet(str(output_parquet))
    assert df.count() == 1
    assert "DATA_INICIO" in df.columns

