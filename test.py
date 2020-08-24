from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement
import os
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext

pathToHere=os.getcwd()

def main():


    #Datastax configuration
    objCC=CassandraConnection()
    cloud_config= {
            'secure_connect_bundle': pathToHere+'\\secure-connect-dbtest.zip'
    }
    
    auth_provider = PlainTextAuthProvider(objCC.cc_user_test,objCC.cc_pwd_test)

    cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
    session = cluster.connect()
    session.default_timeout=70

    #Spark configuration
    querySt="select * from test.tbthesis where period_number>4 ALLOW FILTERING "
    
    conf = SparkConf().setAppName("spark_cassandra_test").setMaster("local")

    sc = SparkContext(conf=conf)
    
    sqlContext = SQLContext(sc).read.format("jdbc") \
                                    .options("url","")         
    print("OK...")








class CassandraConnection():
    cc_user_test='test'
    cc_pwd_test='testquart'
   


if __name__=='__main__':
    main()

