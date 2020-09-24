

1)This repository contains a pyspark/glue job and corresponding test scripts.

**Mainjob**: Glue_Job_SAS_To_Parquet.py

This script was modified in below places

1)changed partition column to changedate,as date will be more appropriate than a timestamp for partitiong.
2)Few pyspark methods were moved to pyspark_return_dataframe.py to remove the dependency of glue context inordered to
   make it work in the local machine .(Getting no module name Dynamic Frame error because of glue) 
   
**Test script**: test_dataframe.py

This script contains two pytests
1)to verify whether creating dataframe functionality working properly
2)if the dataframe is written into the target location.


Pycharm Environment:

1)Use **python 3.7** as the interpreter 
2)**spark-2.2.0-bin-hadoop2.7** library was used for pyspark (add this project content root)
3)Add **py4j** library (spark-2.2.0-bin-hadoop2.7/python/lib/py4j-0.10.4-src.zip to the content root
4)Make **pytest** as the default test runner to execute the pytests.


