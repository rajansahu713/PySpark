from pyspark.sql import SparkSession
spark = SparkSession \
    .builder.config("spark.jars", "mysql-connector-java-8.0.22.jar") \
    .master("local").\
    appName("PySpark_MySQL_test")\
    .getOrCreate()


jdbcDF = spark.read.format("jdbc").\
    option(
            url="jdbc:mysql://localhost/<database_name>",
            driver="com.mysql.jdbc.Driver",
            dbtable="<table_name>",
            user="<user_name>"
            password="<password>").\
load()
jdbcDF.show()