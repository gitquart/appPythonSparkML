import os
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
pathToHere=os.getcwd()
cmd='--packages com.datastax.spark:spark-cassandra-connector_2.11:2.5.1 --files '+os.getcwd()+'\\secure-connect-dbtest.zip pyspark'

#Complete command
#pyspark --packages com.datastax.spark:spark-cassandra-connector_2.11:2.5.1 --files C:\Users\UlysesRico\Documents\quart\appPythonSparkML\secure-connect-dbtest.zip

querySt_1M="select * from test.tbthesis where period_number>4 ALLOW FILTERING "

def main():
    os.environ['PYSPARK_SUBMIT_ARGS'] = cmd
    sc = SparkContext("local","appTest")
    

    

    
   

if __name__=='__main__':
    main()





"""
class CassandraConnection():
    cc_user_test='test'
    cc_pwd_test='testquart'
"""
   




