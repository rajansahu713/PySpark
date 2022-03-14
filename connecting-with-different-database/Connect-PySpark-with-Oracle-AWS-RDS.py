from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.jars", "ojdbc7.jar") \
    .getOrCreate()

jdbcDF = spark.read.format("jdbc").\
options(
         url='jdbc:oracle:thin:admin/oracledbtest@//database-oracle-1.rds.amazonaws.com:1521/<database_name>',
         dbtable='<table_name>',
         user='<user_name>',
         password='<password>',
         driver='oracle.jdbc.OracleDriver').\
load()


jdbcDF.printSchema()