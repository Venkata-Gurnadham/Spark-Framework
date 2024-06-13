from Spark_Data_Profiling.data_sources.file_formats.categorical_cloumn_profiling import categorical_column_profiling
from Spark_Data_Profiling.data_sources.file_formats.numeric_column_profiling import numeric_column_profiling
# from Spark_Data_Profiling.data_sources.file_formats.read_file_data import read_file_formats
from Spark_Data_Profiling.data_sources.read_data import read_file_formats
from Spark_Data_Profiling.data_sources.data_sources_helper import get_column_data_type
from utils.spark_helper import get_spark_session, load_config
from pyspark.sql.types import NumericType, StringType, TimestampType, DateType, BooleanType
# from pyspark.sql.types import ( ArrayType,BooleanType, DateType, DecimalType,DoubleType,FloatType,IntegerType,LongType,StringType,StructField,StructType,TimestampType)

file_path = "Spark_Data_Profiling\\data\\customers-100000.csv"
# config = load_config(config_path)
# spark = get_spark_session(
#         config['spark']['app_name'],
#         config['spark']['master'],
#         config['spark']['config']
#     )
spark = get_spark_session("data_profiling_columns")
file_df = read_file_formats(spark,file_path)
def column_level_profiling(file_df, column_name):
    column_data_type = get_column_data_type(file_df,column_name)
    if column_data_type == "numeric":
        return numeric_column_profiling(params)
    elif column_data_type == "categorical":
         return categorical_column_profiling(params)
    elif column_data_type == "datetime":
         return datetime_column_profiling(params)

# to get column data type 

def get_column_data_type(file_df,column_name):

    if column_name not in file_df.column:
        raise ValueError(f"Column '{column_name}' not found in DataFrame")
    column_data_type = file_df.schema[column_name].dataType
    if isinstance(column_data_type, (NumericType, BooleanType)):
        return "numeric"
    elif isinstance(column_data_type, StringType):
        return "categorical"
    elif isinstance(column_data_type, (TimestampType, DateType)):
        return "datetime"
    else:
        return "unknown column type"

