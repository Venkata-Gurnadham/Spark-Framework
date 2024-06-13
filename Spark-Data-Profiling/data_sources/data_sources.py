# all imports
from Spark_Data_Profiling.data_sources.data_lakes import data_lakes;
from Spark_Data_Profiling.data_sources.data_warehouse import data_warehouse
from Spark_Data_Profiling.data_sources.databases import databases
from Spark_Data_Profiling.data_sources.file_formats import file_formats

def data_source(data_source, parms):
    data_source_map = {
        "databases": databases,
        "file_formats": file_formats,
        "datawarehouse": data_warehouse,
        "datalakes": data_lakes
    }
    
    # get the  function based on the data_source value
    selected_source = data_source_map.get(data_source)
    
    if selected_source:
        return selected_source(parms)
    else:
        print("Please select the correct data source")
