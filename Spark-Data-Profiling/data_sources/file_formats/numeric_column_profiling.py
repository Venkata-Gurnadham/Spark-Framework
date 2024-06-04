from pyspark.sql import functions as F
def numeric_column_profiling(df, column_name):
    if column_name not in df.columns:
        raise ValueError(f"Column '{column_name}' is not in DataFrame")
    else:
        column_data = df.select(column_name)
        total_records = df.count()

        metrics = column_data.agg(
            F.countDistinct(column_name).alias('distinct_count'),
            F.sum(F.when(F.col(column_name).isNull(), 1).otherwise(0)).alias('null_count'),
            F.sum(F.when(F.col(column_name) == 0, 1).otherwise(0)).alias('zero_count'),
            F.mean(column_name).alias('mean'),
            F.min(column_name).alias('min'),
            F.max(column_name).alias('max')
        ).first()

        distinct_count = metrics['distinct_count']
        null_count = metrics['null_count']
        zero_count = metrics['zero_count']
        mean = metrics['mean']
        min_val = metrics['min']
        max_val = metrics['max']

        column_summary = {
            'total_records': total_records,
            'distinct_count': distinct_count,
            'null_count': null_count,
            'zero_count': zero_count,
            'percent_missing': (null_count / total_records) * 100,
            'mean': mean,
            'min': min_val,
            'max': max_val
        }

        return {column_name: column_summary}

