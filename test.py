import os
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
pathToHere=os.getcwd()



def main():

    querySt="select * from test.tbthesis where period_number>4 ALLOW FILTERING "

    #Datastax configuration
    
    # Creating PySpark Context
    sc = SparkContext(conf=SparkConf().setAppName("appTest").setMaster("local"))
    sqlContext = SQLContext(sc)

    table_df = sqlContext.read.format("org.apache.spark.sql.cassandra").options(table='tbthesis', keyspace='test').load()

    print("OK...")    

    #Spark configuration
    
    
  

   

if __name__=='__main__':
    main()





"""
class CassandraConnection():
    cc_user_test='test'
    cc_pwd_test='testquart'
"""
   




