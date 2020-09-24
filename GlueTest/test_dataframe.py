import unittest
import os

import pytest
import pandas as pd
from pyspark_return_dataframe import add_audit_cols,write_to_parquet
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from datetime import datetime


os.environ["PYSPARK_PYTHON"]="/Users/arunbonam/.pyenv/versions/3.7.0/bin/python3.7"
spark = SparkSession.builder.appName('glue').master('local').getOrCreate()
df =spark.createDataFrame(["10","11","13"],"string").toDF("ID")
df.show()


def test_add_audit_cols():

    expected_output_df = spark.createDataFrame(
        [('10','I',str(datetime.now()),'2020-09-24','2020','09','24' ),
         ('11', 'I',str(datetime.now()),'2020-09-24','2020','09','24'),
         ('13', 'I',str(datetime.now()),'2020-09-24','2020','09','24')],
        ['ID','operation','processeddate','changedate','changedate_year','changedate_month','changedate_day'],
    )
    actual_output_df = add_audit_cols(df, '2020-09-24')

    expected_output_df.show()
    actual_output_df.show()

    expected_output = get_sorted_data_frame(
        expected_output_df.toPandas(),
        ['ID']
    )

    actual_output = get_sorted_data_frame(
        expected_output_df.toPandas(),
        ['ID'])

    # Equality assertion

    pd.testing.assert_frame_equal(
        expected_output,
        actual_output,
        check_like=True,
    )


def get_sorted_data_frame(data_frame, columns_list):
    return data_frame.sort_values(columns_list).reset_index(drop=True)


def test_write_to_parquet():
    actual_output_df = add_audit_cols(df, '2020-09-24')
    partition_cols = ['changedate']
    target_path='/Users/arunbonam/Desktop/spark'
    write_to_parquet(actual_output_df,partition_cols,target_path)
    df_from_target = spark.read.parquet(target_path+'/changedate=2020-09-24/part-*.parquet')
    assert df_from_target!= None





