import os
from pyspark import SparkContext,SparkFiles,SQLContext
from pyspark.sql import SQLContext, SparkSession
pathToHere=os.getcwd()

#Complete command
#pyspark --packages com.datastax.spark:spark-cassandra-connector_2.11:2.5.1 --files C:\Users\UlysesRico\Documents\quart\appPythonSparkML\secure-connect-dbtest.zip

#querySt_1M="select * from test.tbthesis where period_number>4 ALLOW FILTERING "
#os.environ['JAVA_HOME']='C:\\Program Files\\Java\\jdk1.8.0_261'
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--files '+os.getcwd()+'\\secure-connect-dbtest.zip pyspark'

def main():
   
    session = SparkSession.builder.appName('SparkCassandraApp').config('spark.cassandra.connection.config.cloud.path',os.getcwd()+'\\secure-connect-dbtest.zip').config('spark.cassandra.auth.username','test').config('spark.cassandra.auth.password','testquart').getOrCreate()
    sqlContext=SQLContext(session)
    
    


if __name__=='__main__':
    main()




   




