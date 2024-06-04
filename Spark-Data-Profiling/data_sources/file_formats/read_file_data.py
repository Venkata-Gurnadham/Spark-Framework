
def read_file_formats(spark,filepath):
    format = filepath.split(".")[-1].lower()
    supported_formats = {
        "csv": spark.read.csv,
        "parquet": spark.read.parquet,
        "json": spark.read.json,
        "orc": spark.read.orc,
        "txt": spark.read.text
    }

    try:
        read_func = supported_formats[format]
        df = read_func(filepath).cache()
        return df
    except KeyError:
        raise ValueError(f"Unsupported file format: {format}")