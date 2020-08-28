import os
from pyspark import SparkContext,SparkFiles,SQLContext
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.functions import col

#querySt_1M="select * from test.tbthesis where period_number>4 ALLOW FILTERING "
#pyspark shell is pyspark command

def main():
    
    #os.environ['JAVA_HOME']='C:\\JAVA'
    #os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.12:2.5.0 --master spark://192.168.1.66:4040/ pyspark'
    
    secure_bundle_file=os.getcwd()+'\\secure-connect-dbtest.zip'
    sparkSession = SparkSession.builder.appName('SparkCassandraApp').config('spark.cassandra.connection.config.cloud.path',secure_bundle_file).config('spark.cassandra.auth.username', 'test').config('spark.cassandra.auth.password','testquart').config('spark.dse.continuousPagingEnabled',False).config('spark.cassandra.connection.host','c58d5d9e-015a-4255-8f6c-05784a7c59ba-us-east1.db.astra.datastax.com').config("com.datastax.spark:spark-cassandra-connector_2.11:2.5.1").master('local[*]').getOrCreate()
    #Until here is fine, the "reading" is failing
    data = sparkSession.read.format("org.apache.spark.sql.cassandra").options(table="tbthesis", keyspace="test").load()
    data.count()
    

if __name__=='__main__':
    main()




   




