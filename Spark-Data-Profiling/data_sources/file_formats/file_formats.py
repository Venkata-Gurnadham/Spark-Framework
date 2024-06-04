
from Spark_Data_Profiling.data_sources.file_formats.column_profiling import column_level_profiling
from Spark_Data_Profiling.data_sources.file_formats.dataset_profiling import dataset_level_profiling
# reading file formats
# fetching file formats
def file_format(parms, data_profiling_level):
    # Implementation for file format
    if data_profiling_level == 'column_level_profiling':
        return column_level_profiling(parms)
    elif data_profiling_level == "dataset_level_profiling":
        return dataset_level_profiling(parms)
    else:
        print("Please select the profiling level")
# def file_format(parms):
#     # Implementation for file format
#     # if data_profiling_level = 'structural_profiling':
#     #     structural_profiling = structural_level_profiling(params)
#     # distinct_count, null_count, value_distribution, min/max values, statistical 
#     if data_profiling_level = 'column_level_profiling':
#         column_level_profiling = column_level_profiling(params)
#     #rowcount,duplicate_count
#     elif data_profiling_level = "dataset_level_profiling":
#        dataset_level_profiling = dataset_level_profiling(params)
#     else:
#         print("please select the Profiling level")