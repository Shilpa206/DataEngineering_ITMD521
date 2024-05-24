from pyspark.sql.types import *
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


if __name__ == "__main__":
    print("======== Assignment Part#1")
    # Create Spark session
    spark = (
        SparkSession
        .builder
        .appName("assignment_04")
        .getOrCreate()
    )        

    # Read employees table into a DataFrame
    employees_df = (
        spark
        .read
        .format("jdbc")
        .option("url", "jdbc:mysql://localhost:3306/employees")
        .option("driver", "com.mysql.jdbc.Driver")
        .option("dbtable", "employees")
        .option("user", "worker")
        .option("password", "cluster")
        .load()
    )

    print(f"\n======== Count of records in the employees_df: {employees_df.count()}")
    print("\n======== Schema of employees_df")
    employees_df.printSchema()


    # Create a DataFrame of top 10,000 employee salaries sorted in descending order
    salaries_df = (
        spark.read
        .format("jdbc")
        .option("url", "jdbc:mysql://localhost:3306/employees")
        .option("dbtable", "salaries")
        .option("user", "worker")
        .option("password", "cluster")
        .load()
    )

    aces_df = salaries_df.orderBy(col("salary").desc()).limit(10000)
    
    # Write DataFrame back to database to a new table called 'aces'
    ( 
        aces_df.write
        .format("jdbc")
        .option("url", "jdbc:mysql://localhost:3306/employees")
        .option("dbtable", "aces")
        .option("user", "worker")
        .option("password", "cluster")
        .mode("overwrite")
        .save()
    )

    # Write DataFrame out to local system as a CSV with Snappy compression
    (
        aces_df.write
        .format("csv")
        .option("header", "true")
        .option("compression", "snappy")
        .mode("overwrite")
        .save("aces.csv")
    )

    print("\n======== Assignment Part#2")
    sr_engineers_sql = """
    SELECT emp_no, title, from_date, to_date
    FROM titles 
    WHERE title = 'Senior Engineer'
    """
    sr_engineers_df = (
        spark.read
        .format("jdbc")
        .option("url", "jdbc:mysql://localhost:3306/employees")
        .option("user", "worker")
        .option("password", "cluster")
        .option("query", sr_engineers_sql)
        .load()
    )

    sr_engineers_status_df = (
        sr_engineers_df
        .withColumn(
            "status",
            when(col("to_date") == "9999-01-01", "current")
            .otherwise("left")
        )
    )

    print("\n======== Count of how many senior engineers have left and how many are current")
    sr_engineers_status_df.groupBy("status").count().show()

    # Create a PySpark SQL table of just the Senior Engineers information that have left the company
    (
        sr_engineers_status_df
        .where("status = 'left'")
        .select("emp_no", "title", "from_date", "to_date")
        .write
        .mode("overwrite")
        .option("path", "left_table_files")
        .saveAsTable("left_spark_sql_table")
    )

    # Create a PySpark SQL tempView of just the Senior Engineers information that have left the company
    (
        sr_engineers_status_df
        .where("status = 'left'")
        .select("emp_no", "title", "from_date", "to_date")
        .createOrReplaceTempView("left_spark_sql_temp_view")
    )
    
    # Create a PySpark DataFrame of just the Senior Engineers information that have left the company
    left_spark_df = (
        sr_engineers_status_df
        .where("status = 'left'")
        .select("emp_no", "title", "from_date", "to_date")
    )


    # Write to DB 
    # Spark SQL Table 
    ( 
        spark.sql("SELECT * FROM left_spark_sql_table").write
        .format("jdbc")
        .option("url", "jdbc:mysql://localhost:3306/employees")
        .option("dbtable", "left_table")
        .option("user", "worker")
        .option("password", "cluster")
        .mode("overwrite")
        .save()
    )

    # Spark SQL Temp View 
    ( 
        spark.sql("SELECT * FROM left_spark_sql_temp_view").write
        .format("jdbc")
        .option("url", "jdbc:mysql://localhost:3306/employees")
        .option("dbtable", "left_tempview")
        .option("user", "worker")
        .option("password", "cluster")
        .mode("overwrite")
        .save()
    )

    # Spark DF
    ( 
        left_spark_df
        .write
        .format("jdbc")
        .option("url", "jdbc:mysql://localhost:3306/employees")
        .option("dbtable", "left_df")
        .option("user", "worker")
        .option("password", "cluster")
        .mode("overwrite")
        .save()
    )

    # Spark DF write 
    ( 
        left_spark_df
        .write
        .format("jdbc")
        .option("url", "jdbc:mysql://localhost:3306/employees")
        .option("dbtable", "left_df")
        .option("user", "worker")
        .option("password", "cluster")
        .mode("errorifexists")
        .save()
    )