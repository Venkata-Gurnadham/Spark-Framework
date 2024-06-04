from Spark_Data_Profiling.utils.spark_helper import load_config
config_file = 'Spark_Data_Profiling\config\config.yaml'
config = load_config(config_file)
input_data_path = config['data']['input_path']


def read_data(spark,data_source):
    if data_source == 'file_formats':
        read_file_formats(spark,input_data_path)
    elif data_source == 'datawarehouse':
        read_warehouses(spark, params)
    elif data_source == 'databases':
        read_databases(spark, params)
    elif data_source == 'data_lakes':
        read_datalakes(spark,params)

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
def read_databases(params):
    pass
def read_warehouses(params):
    pass
def read_datalakes(params):
    pass