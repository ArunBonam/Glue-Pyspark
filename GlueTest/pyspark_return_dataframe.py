
import sys
import pyspark.sql.functions as f
from pyspark.context import SparkContext

def add_audit_cols(df, changedt):
    """ Adds audit columns to the dataframe
    """
    df = df.withColumn("operation", f.lit("I")) \
           .withColumn("processeddate", f.current_timestamp().cast("String")) \
           .withColumn("changedate", f.lit(changedt)) \
           .withColumn('changedate_year', f.year('changedate').cast("String")) \
           .withColumn('changedate_month', f.month('changedate').cast("String")) \
           .withColumn('changedate_day', f.dayofmonth('changedate').cast("String"))
    return df


def write_to_parquet(df, partition_cols,target_path,mode="overwrite"):
    print("write data to desktop")
    df.write.mode(mode).format("parquet").partitionBy(partition_cols).save(target_path)



