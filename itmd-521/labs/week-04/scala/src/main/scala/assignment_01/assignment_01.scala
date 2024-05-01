// scalastyle:off println

package main.scala.assignment_01

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
  * Usage: assignment_01 <divvy_file_dataset>
  */
object assignment_01 {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("assignment_01")
      .getOrCreate()

    if (args.length < 1) {
      println("Usage: assignment-01 <divvy_file_dataset>")
      sys.exit(1)
    }

    // get the divvy data set file name
    val divvy_file = args(0)

    // read the file into a Spark DataFrame
    val df1 = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(divvy_file)

    println("======== DF1_Inferred_Schema: Inferred Schema:")
    df1.printSchema()
    df1.show(10, truncate=false)
    println("======== DF1_Inferred_Schema Record Count:", df1.count())

    // create df2 programmatically using StructFields to create and attach a schema
    val schema = StructType(
        Array(
            StructField("trip_id", IntegerType, true),
            StructField("starttime", StringType, true),
            StructField("stoptime", StringType, true),
            StructField("bikeid", IntegerType, true),
            StructField("tripduration", IntegerType, true),
            StructField("from_station_id", IntegerType, true),
            StructField("from_station_name", StringType, true),
            StructField("to_station_id", IntegerType, true),
            StructField("to_station_name", StringType, true),
            StructField("usertype", StringType, true),
            StructField("gender", StringType, true),
            StructField("birthyear", IntegerType, true)
        )
    )

    // read divvy csv file into df2
    val df2 = spark.read.format("csv")
      .option("header", "true")
      .schema(schema)
      .load(divvy_file)

    println("======== DF2_Struct_Schema: Schema Created Programmatically using StructFields")
    df2.printSchema()
    println("======== DF2_Struct_Schema Record Count:", df2.count())
    
    // create df3 by ddl. Attach a schema via DDL and read the CSV file
    val schema_ddl = "trip_id INT, starttime STRING, stoptime STRING, bikeid INT, tripduration INT, from_station_id INT, from_station_name STRING, to_station_id INT, to_station_name STRING, usertype STRING, gender STRING, birthyear INT"
    // read csv file into df3
    val df3 = spark.read.format("csv")
      .option("header", "true")
      .schema(schema_ddl)
      .load(divvy_file)

    println("======== DF3_DDL_Schema: Schema Attached via DDL")
    df3.printSchema()
    println("======== DF3_DDL_Schema Record Count:", df3.count())


    // Tranformations and actions. Select Gender Female, Groupby to station name
    // For df1
    val df1_transf_act = df1
        .select("gender", "to_station_name")
        .where(df1("gender") === "Female")
        .groupBy("to_station_name")
        .count()

    // show all the resulting aggregation for the df1_transf_act
    df1_transf_act.show(10, truncate=false)


    // For df2
    val df2_transf_act = df2
        .select("gender", "to_station_name")
        .where(df2("gender") === "Female")
        .groupBy("to_station_name")
        .count()                                 
    // show all the resulting aggregation for the df1_transf_act
    df2_transf_act.show(10, truncate=false)


    // For df3
    val df3_transf_act = df3
        .select("gender", "to_station_name")
        .where(df3("gender") === "Female")
        .groupBy("to_station_name")
        .count()
                                       
    // show all the resulting aggregation for the df1_transf_act
    df3_transf_act.show(10, truncate=false)

  }
}