from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.jars", "ojdbc7.jar") \
    .getOrCreate()

jdbcDF = spark.read.format("jdbc").\
option(
      url= "jdbc:oracle:thin:@localhost:1521:<database_name>",
      dbtable='<table_name>',
      user='<user_name>',
      password='<password>',
      driver='oracle.jdbc.OracleDriver').\
load()

jdbcDF.show()