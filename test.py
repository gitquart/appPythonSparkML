import os
from pyspark import SparkContext,SparkFiles,SQLContext
from pyspark.sql import SQLContext, SparkSession


#querySt_1M="select * from test.tbthesis where period_number>4 ALLOW FILTERING "


def main():
      
    secure_bundle_file=os.getcwd()+'\\secure-connect-dbtest.zip'
    sparkSession = SparkSession.builder.appName('SparkCassandraApp').config('spark.cassandra.connection.config.cloud.path',secure_bundle_file).config('spark.cassandra.auth.username', 'test').config('spark.cassandra.auth.password','testquart').config('spark.dse.continuousPagingEnabled',False).master('local[*]').getOrCreate()
    dataframe = sparkSession.read.format("org.apache.spark.sql.cassandra").options(table='tbthesis',keyspace='test').load()
    dataframe.count()
    print('ok')
    

if __name__=='__main__':
    main()




   




