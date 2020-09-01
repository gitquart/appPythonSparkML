import os
from pyspark import SparkContext
from pyspark.sql import  SparkSession,SQLContext


#querySt_1M="select * from test.tbthesis where period_number>4 ALLOW FILTERING "
secure_bundle_file=os.getcwd()+'\\secure-connect-dbtest.zip'

def main():
      
   
    sparksn = SparkSession.builder.appName('SparkCassandraApp').config('spark.cassandra.connection.config.cloud.path',secure_bundle_file).config('spark.cassandra.auth.username', 'test').config('spark.cassandra.auth.password','testquart').config('spark.dse.continuousPagingEnabled',False).master('local[*]').getOrCreate()
    #sqlCxt= SQLContext(sparksn)
    #The problem is coming at load()
    df=sparksn.read.format("org.apache.spark.sql.cassandra").options(table='tbthesis',keyspace='test').load()
    df.count()
    print('ok')
    

if __name__=='__main__':
    main()




   




