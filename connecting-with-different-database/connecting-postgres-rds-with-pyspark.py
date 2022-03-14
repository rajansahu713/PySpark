from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.jars", "postgresql-42.2.14.jar") \
    .getOrCreate()

jdbcDF = spark.read.format("jdbc").\
options(
         url='jdbc:postgresql://nbj-instance.crjyrm6gyucn.-1.rds.amazonaws.com/<database_name>', 
         dbtable='<table_name>',
         user='<user_name>',
         password='<password>',
         driver='org.postgresql.Driver').\
load()
jdbcDF.show()