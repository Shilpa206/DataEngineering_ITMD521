from pyspark.sql.types import *
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: assignment_03 <departure_delays_csv_file>", file=sys.stderr)
        sys.exit(-1)

    # get the sf_fire_calls data set file name
    departure_delays_csv_file = sys.argv[1]

    # Create Spark session
    spark = (
        SparkSession
        .builder
        .appName("DepartureDelays")
        .getOrCreate()
    )        

    #creating departure delays schema by DDl. Attaching the schema via DDL and reading the CSV file
    departure_delays_schema_ddl = "date STRING, delay INT, distance INT, origin STRING, destination STRING"
    # read departure delay csv file into df
    departure_delays_df = (
        spark
        .read
        .csv(departure_delays_csv_file, header=True, schema=departure_delays_schema_ddl)
    )

    print("\n======== DF_DDL_Schema: Schema Attached via DDL")
    departure_delays_df.printSchema()


    departure_delays_df.createOrReplaceTempView("departure_delays_v")

    #Questions:

    print("####### PART 1:")

    # Spark SQL
    query_1_sql = """
    SELECT 
        date,
        delay,
        origin, 
        destination
    FROM 
        departure_delays_v
    WHERE 
        delay > 120 
        AND ORIGIN = 'SFO'
        AND DESTINATION = 'ORD'
    ORDER by delay DESC
    """

    query_1_sql_df = spark.sql(query_1_sql)
    query_1_sql_df.show(10)

    # PySpark 
    query_1_pyspark_df = (
        departure_delays_df
        .filter(
            (col("delay") > 120) 
            & (col("ORIGIN") == "SFO")
            & (col("DESTINATION") == "ORD")
        )
        .orderBy(col("delay").desc())
        .select(col("date"), col("delay"), col("origin"),  col("destination"))
    )
    query_1_pyspark_df.show(10)


    # Query 2 
    query_2_sql = """
    SELECT 
        delay, 
        origin, 
        destination,
        CASE
            WHEN delay > 360 THEN 'Very Long Delays'
            WHEN delay > 120 AND delay < 360 THEN 'Long Delays'
            WHEN delay > 60 AND delay < 120 THEN 'Short Delays'
            WHEN delay > 0 and delay < 60 THEN 'Tolerable Delays'
            WHEN delay = 0 THEN 'No Delays'
            ELSE 'Early'
        END AS Flight_Delays
    FROM 
        departure_delays_v
    ORDER BY 
        origin, 
        delay DESC
    """

    query_2_sql_df = spark.sql(query_2_sql)
    query_2_sql_df.show(10)

    query_2_pyspark_df = (
        departure_delays_df
        .orderBy(col("origin"), col("delay").desc())
        .select(
            col("delay"), 
            col("origin"),  
            col("destination"),
            when(col("delay") > 360, "Very Long Delays")
            .when((col("delay") > 120) &( col("delay") < 360), "Long Delays")
            .when((col("delay") > 60) & (col("delay") < 120), "Short Delays")
            .when((col("delay") > 0) & (col("delay") < 60), "Tolerable Delays")
            .when(col("delay") == 0, "No Delays")
            .otherwise("Early")
            .alias("Flight_Delays")
        )
    )
    query_2_pyspark_df.show(10)


    print("####### PART 2:")
    # Drop table if already exists 
    spark.sql("DROP TABLE IF EXISTS us_delay_flights_tbl")

    # Create table 
    departure_delays_df.write.saveAsTable("us_delay_flights_tbl")

    #  Create view 
    create_view_sql = """
    CREATE OR REPLACE TEMP VIEW ord_O301_0315 AS
    SELECT date, delay, distance, origin, destination
    FROM us_delay_flights_tbl 
    WHERE origin = 'ORD' 
    AND cast(substring(date, 1, 4) as int) BETWEEN 301 AND 315
    """
    spark.sql(create_view_sql)

    # Query from view 
    spark.sql("SELECT * FROM ord_O301_0315").show(5)


    # List columns in table  
    print("========== List Columns")
    spark.catalog.listColumns("us_delay_flights_tbl")


    print("####### PART 3:")
    part3_df = departure_delays_df.withColumn("date", to_date(col("date"), "MMddHHmm"))

    # Write as JSON 
    part3_df.write.mode("overwrite").json("departuredelays")

    # Write as JSON with lz4 compression 
    part3_df.write.mode("overwrite").option("compression", "lz4").json("departuredelays")

    # Write as Parquet 
    part3_df.write.mode("overwrite").parquet("departuredelays")


    print("####### PART 4:")
    parquet_df = spark.read.parquet("departuredelays")
    ord_df = parquet_df.filter(col("origin") == "ORD")

    ord_df.show(10)

    ord_df.write.mode("overwrite").format("parquet").save("orddeparturedelays")