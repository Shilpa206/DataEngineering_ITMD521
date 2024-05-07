// scalastyle:off println

package main.scala.assignment_03

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


object assignment_03 {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("assignment_03")
      .getOrCreate()

    if (args.length < 1) {
      println("Usage: assignment-03 <file_path>")
      sys.exit(1)
    }

    val departure_delays_csv_file = args(0)

    import spark.implicits._

    // creating departure delays schema by DDl. Attaching the schema via DDL and reading the CSV file
    val departure_delays_schema_ddl = "date STRING, delay INT, distance INT, origin STRING, destination STRING"
    // read departure delay csv file into df
    val departure_delays_df = spark
        .read
        .option("header", true)
        .schema(departure_delays_schema_ddl)
        .csv(departure_delays_csv_file)

    println("======== DF_DDL_Schema: Schema Attached via DDL")
    departure_delays_df.printSchema()


    departure_delays_df.createOrReplaceTempView("departure_delays_v")

    // Questions:

    println("####### PART 1:")

    // Spark SQL
    val query_1_sql = """
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

    val query_1_sql_df = spark.sql(query_1_sql)
    query_1_sql_df.show(10)

    // PySpark 
    val query_1_pyspark_df = departure_delays_df
        .filter(
            $"delay" > 120
            && $"ORIGIN" === "SFO"
            && $"DESTINATION" === "ORD"
        )
        .orderBy($"delay".desc)
        .select($"date", $"delay", $"origin", $"destination")
    query_1_pyspark_df.show(10)


    // Query 2 
    val query_2_sql = """
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

    val query_2_sql_df = spark.sql(query_2_sql)
    query_2_sql_df.show(10)

    val query_2_pyspark_df = (
        departure_delays_df
        .orderBy($"origin", $"delay".desc)
        .select(
            $"delay", 
            $"origin",  
            $"destination",
            when($"delay" > 360, "Very Long Delays")
            .when(($"delay" > 120) && ($"delay" < 360), "Long Delays")
            .when(($"delay" > 60) && ($"delay" < 120), "Short Delays")
            .when(($"delay" > 0) && ($"delay" < 60), "Tolerable Delays")
            .when($"delay" === 0, "No Delays")
            .otherwise("Early")
            .alias("Flight_Delays")
        )
    )
    query_2_pyspark_df.show(10)


    println("####### PART 2:")
    // Drop table if already exists 
    spark.sql("DROP TABLE IF EXISTS us_delay_flights_tbl")

    // Create table 
    departure_delays_df.write.saveAsTable("us_delay_flights_tbl")

    //  Create view 
    val create_view_sql = """
    CREATE OR REPLACE TEMP VIEW ord_O301_0315 AS
    SELECT date, delay, distance, origin, destination
    FROM us_delay_flights_tbl 
    WHERE origin = 'ORD' 
    AND cast(substring(date, 1, 4) as int) BETWEEN 301 AND 315
    """
    spark.sql(create_view_sql)

    // Query from view 
    spark.sql("SELECT * FROM ord_O301_0315").show(5)


    // List columns in table  
    println("========== List Columns")
    println(spark.catalog.listColumns("us_delay_flights_tbl"))


    println("####### PART 3:")
    val part3_df = departure_delays_df.withColumn("date", to_date($"date", "MMddHHmm"))

    // Write as JSON 
    part3_df.write.mode("overwrite").json("departuredelays")

    // Write as JSON with lz4 compression 
    part3_df.write.mode("overwrite").option("compression", "lz4").json("departuredelays")

    // Write as Parquet 
    part3_df.write.mode("overwrite").parquet("departuredelays")


    println("####### PART 4:")
    val parquet_df = spark.read.parquet("departuredelays")
    val ord_df = parquet_df.filter($"origin" === "ORD")

    ord_df.show(10)

    ord_df.write.mode("overwrite").format("parquet").save("orddeparturedelays")

  }
}