import os
from pyspark import SparkContext,SparkFiles,SQLContext,SparkFiles
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.functions import col



#Complete command
#pyspark  --files C:\Users\UlysesRico\Documents\quart\appPythonSparkML\secure-connect-dbtest.zip
#querySt_1M="select * from test.tbthesis where period_number>4 ALLOW FILTERING "

def main():
   
    
    secure_bundle_file=os.getcwd()+'\\secure-connect-dbtest.zip'
    sparkSession = SparkSession.builder.appName('SparkCassandraApp').config('spark.cassandra.connection.config.cloud.path',secure_bundle_file).config('spark.cassandra.auth.username', 'test').config('spark.cassandra.auth.password','testquart').config('spark.dse.continuousPagingEnabled',False).master('local[*]').getOrCreate()
    #Until here is fine, the "reading" is failing
    data = sparkSession.read.format("org.apache.spark.sql.cassandra").options(table="tbthesis", keyspace="test").load()
    data.count()
    
    


if __name__=='__main__':
    main()




   




