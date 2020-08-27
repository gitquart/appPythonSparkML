import os
from pyspark import SparkContext,SparkFiles,SQLContext,SparkFiles
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.functions import col


#This package was downloaded without errors in C:\Users\UlysesRico\.ivy2\jars
#--packages com.datastax.spark:spark-cassandra-connector_2.11:2.0.0-M3
#querySt_1M="select * from test.tbthesis where period_number>4 ALLOW FILTERING "
#pyspark shell is pyspark command



def main():
    
    #os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.11:2.0.0-M3  pyspark'
    secure_bundle_file=os.getcwd()+'\\secure-connect-dbtest.zip'
    sparkSession = SparkSession.builder.appName('SparkCassandraApp').config('spark.cassandra.connection.config.cloud.path',secure_bundle_file).config('spark.cassandra.auth.username', 'test').config('spark.cassandra.auth.password','testquart').config('spark.dse.continuousPagingEnabled',False).config('spark.cassandra.connection.host','c58d5d9e-015a-4255-8f6c-05784a7c59ba-us-east1.db.astra.datastax.com').master('local[2]').getOrCreate()
    #Until here is fine, the "reading" is failing
    data = sparkSession.read.format("org.apache.spark.sql.cassandra").options(table="tbthesis", keyspace="test").load()
    data.count()
    
    


if __name__=='__main__':
    main()




   




