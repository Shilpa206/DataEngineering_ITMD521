from pyspark.sql.types import *
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: assignment_02 <sf_fire_calls_file>", file=sys.stderr)
        sys.exit(-1)

    # get the sf_fire_calls data set file name
    sf_fire_calls_file = sys.argv[1]

    # Create Spark session
    spark = (
        SparkSession
        .builder
        .appName("SFfireCalls")
        .getOrCreate()
    )

    #create dataframe programmatically using StructFields to create and attach a schema
    fire_schema = StructType(
        [
            StructField('CallNumber', IntegerType(), True),
            StructField('UnitID', StringType(), True),
            StructField('IncidentNumber', IntegerType(), True),
            StructField('CallType', StringType(), True),
            StructField('CallDate', StringType(), True),
            StructField('WatchDate', StringType(), True),
            StructField('CallFinalDisposition', StringType(), True),
            StructField('AvailableDtTm', StringType(), True),
            StructField('Address', StringType(), True),
            StructField('City', StringType(), True),
            StructField('Zipcode', IntegerType(), True),
            StructField('Battalion', StringType(), True),
            StructField('StationArea', StringType(), True),
            StructField('Box', StringType(), True),
            StructField('OriginalPriority', StringType(), True),
            StructField('Priority', StringType(), True),
            StructField('FinalPriority', IntegerType(), True),
            StructField('ALSUnit', BooleanType(), True),
            StructField('CallTypeGroup', StringType(), True),
            StructField('NumAlarms', IntegerType(), True),
            StructField('UnitType', StringType(), True),
            StructField('UnitSequenceInCallDispatch', IntegerType(), True),
            StructField('FirePreventionDistrict', StringType(), True),
            StructField('SupervisorDistrict', StringType(), True),
            StructField('Neighborhood', StringType(), True),
            StructField('Location', StringType(), True),
            StructField('RowID', StringType(), True),
            StructField('Delay', FloatType(), True)
        ]
    )

    #read sf_fire_calls_file into df
    df = spark.read.csv(sf_fire_calls_file, header=True, schema=fire_schema)
    
    #print the fire_schema using printshema function
    print("\n======== DF_Fire_Schema: Schema Created Programmatically using StructFields")
    df.printSchema()
    #print("======== DF_Fire_Schema Record Count:", df.count())



    #Questions:
    
    #Q1.What were all the different types of fire calls in 2018?

    ##########################      ANSWER        ##############################
    
    types_of_firecalls_in_2018 = (
        df
        .withColumn("CallDateAsDate", to_date(col("CallDate"), "MM/dd/yyyy"))
        .filter(year(col("CallDateAsDate")) == 2018)
        .select("CallType") 
        .distinct()
    )
    types_of_firecalls_in_2018.show(n=10, truncate=False)

    """
    output:
    +-----------------------------+
    |CallType                     |
    +-----------------------------+
    |Elevator / Escalator Rescue  |
    |Alarms                       |
    |Odor (Strange / Unknown)     |
    |Citizen Assist / Service Call|
    |HazMat                       |
    |Vehicle Fire                 |
    |Other                        |
    |Outside Fire                 |
    |Traffic Collision            |
    |Assist Police                |
    +-----------------------------+
    only showing top 10 rows
    """



    
    #Q2.What months within the year 2018 saw the highest number of fire calls?

    ##########################      ANSWER        ##############################
    
    highest_firecalls_in_2018_months = (
        df
        .withColumn("CallDateAsDate", to_date(col("CallDate"), "MM/dd/yyyy"))
        .filter(year(col("CallDateAsDate")) == 2018)
        .withColumn("CallMonth", month(col("CallDateAsDate")))
        .groupBy("CallMonth") 
        .count()
        .orderBy(col("count").desc())
    )
    highest_firecalls_in_2018_months.show(n=10, truncate=False)

    """
    output:
    +---------+-----+
    |CallMonth|count|
    +---------+-----+
    |10       |1068 |
    |5        |1047 |
    |3        |1029 |
    |8        |1021 |
    |1        |1007 |
    |6        |974  |
    |7        |974  |
    |9        |951  |
    |4        |947  |
    |2        |919  |
    +---------+-----+
    only showing top 10 rows
    """



    #Q3.Which neighborhood in San Francisco generated the most fire calls in 2018?

    ##########################      ANSWER        ##############################
    
    highest_firecalls_in_2018_neighborhood = (
        df
        .withColumn("CallDateAsDate", to_date(col("CallDate"), "MM/dd/yyyy"))
        .filter(
            (year(col("CallDateAsDate")) == 2018) & (col("City") == "San Francisco")
        )    
        .groupBy("Neighborhood") 
        .count()
        .orderBy(col("count").desc())
    )
    highest_firecalls_in_2018_neighborhood.show(n=10, truncate=False)

    """
    output:
    +------------------------------+-----+
    |Neighborhood                  |count|
    +------------------------------+-----+
    |Tenderloin                    |1393 |
    |South of Market               |1052 |
    |Mission                       |911  |
    |Financial District/South Beach|764  |
    |Bayview Hunters Point         |513  |
    |Western Addition              |351  |
    |Sunset/Parkside               |345  |
    |Nob Hill                      |289  |
    |Hayes Valley                  |288  |
    |Outer Richmond                |262  |
    +------------------------------+-----+
    only showing top 10 rows
    """



    #Q4.Which neighborhoods had the worst response times to fire calls in 2018?

    ##########################      ANSWER        ##############################
    
    worse_response_times_in_2018_neighborhood = (
        df
        .withColumn("CallDateAsDate", to_date(col("CallDate"), "MM/dd/yyyy"))
        .filter(year(col("CallDateAsDate")) == 2018)    
        .groupBy("Neighborhood")
        .agg(avg("delay").alias("response_time"))
        .orderBy(col("response_time").desc())
    )
    worse_response_times_in_2018_neighborhood.show(n=10, truncate=False)


    """
    output:
    +------------------------------+-----------------+
    |Neighborhood                  |response_time    |
    +------------------------------+-----------------+
    |Chinatown                     |6.190314101143033|
    |Presidio                      |5.829227011272873|
    |Treasure Island               |5.453703684111436|
    |McLaren Park                  |4.74404764175415 |
    |Bayview Hunters Point         |4.620561962212182|
    |Presidio Heights              |4.594131482319093|
    |Inner Sunset                  |4.438095217981896|
    |Inner Richmond                |4.364728709292966|
    |Financial District/South Beach|4.344084616885593|
    |Haight Ashbury                |4.266428579390049|
    +------------------------------+-----------------+
    only showing top 10 rows
    """




    #Q5.Which week in the year in 2018 had the most fire calls?

    ##########################      ANSWER        ##############################
    
    highest_firecalls_in_2018_week = (
        df
        .withColumn("CallDateAsDate", to_date(col("CallDate"), "MM/dd/yyyy"))
        .filter(year(col("CallDateAsDate")) == 2018)
        .withColumn("CallWeekInTheYear", weekofyear(col("CallDateAsDate")))
        .groupBy("CallWeekInTheYear") 
        .count()
        .orderBy(col("count").desc())
    )
    highest_firecalls_in_2018_week.show(n=10, truncate=False)


    """
    output:
    +-----------------+-----+
    |CallWeekInTheYear|count|
    +-----------------+-----+
    |22               |259  |
    |40               |255  |
    |43               |250  |
    |25               |249  |
    |1                |246  |
    |44               |244  |
    |13               |243  |
    |32               |243  |
    |11               |240  |
    |18               |236  |
    +-----------------+-----+
    only showing top 10 rows
    """




    #Q6.Is there a correlation between neighborhood, zip code, and number of fire calls?

    ##########################      ANSWER        ##############################

    corr_neighborhood_zip_fire_calls = (
        df
        .groupBy("Neighborhood", "Zipcode") 
        .count()
        .orderBy(col("count").desc())
    )
    corr_neighborhood_zip_fire_calls.show(n=10, truncate=False)

    """
    output:
    +------------------------------+-------+-----+
    |Neighborhood                  |Zipcode|count|
    +------------------------------+-------+-----+
    |Tenderloin                    |94102  |17084|
    |South of Market               |94103  |13762|
    |Mission                       |94110  |10444|
    |Bayview Hunters Point         |94124  |9150 |
    |Mission                       |94103  |5445 |
    |Tenderloin                    |94109  |5377 |
    |Financial District/South Beach|94105  |4235 |
    |Outer Richmond                |94121  |4121 |
    |Nob Hill                      |94109  |3983 |
    |Castro/Upper Market           |94114  |3946 |
    +------------------------------+-------+-----+
    only showing top 10 rows
    """



    #Q7.How can we use Parquet files or SQL tables to store this data and read it back?

    ##########################      ANSWER        ##############################

    # Save as parquet 
    parquet_path = "sf-fire-calls.parquet"
    df.write.mode("overwrite").parquet(parquet_path)

    # Read Parquet 
    parquet_df = spark.read.parquet(parquet_path)
    parquet_df.printSchema()

    # Save as SQL table 
    table_name = "sf_fire_calls"
    # Delete table if already present 
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")

    df.write.format("parquet").saveAsTable(table_name)

    # Read from SQL 
    spark.sql(
        """
        SELECT Neighborhood, COUNT(1) AS NumberOfCalls
        FROM sf_fire_calls 
        GROUP BY Neighborhood 
        ORDER BY COUNT(1) DESC
        LIMIT 10
        """
    ).show(10)


    """
    output for parquet_df.printSchema() after reading the parquet file into this parquet_df:

    |-- CallNumber: integer (nullable = true)
    |-- UnitID: string (nullable = true)
    |-- IncidentNumber: integer (nullable = true)
    |-- CallType: string (nullable = true)
    |-- CallDate: string (nullable = true)
    |-- WatchDate: string (nullable = true)
    |-- CallFinalDisposition: string (nullable = true)
    |-- AvailableDtTm: string (nullable = true)
    |-- Address: string (nullable = true)
    |-- City: string (nullable = true)
    |-- Zipcode: integer (nullable = true)
    |-- Battalion: string (nullable = true)
    |-- StationArea: string (nullable = true)
    |-- Box: string (nullable = true)
    |-- OriginalPriority: string (nullable = true)
    |-- Priority: string (nullable = true)
    |-- FinalPriority: integer (nullable = true)
    |-- ALSUnit: boolean (nullable = true)
    |-- CallTypeGroup: string (nullable = true)
    |-- NumAlarms: integer (nullable = true)
    |-- UnitType: string (nullable = true)
    |-- UnitSequenceInCallDispatch: integer (nullable = true)
    |-- FirePreventionDistrict: string (nullable = true)
    |-- SupervisorDistrict: string (nullable = true)
    |-- Neighborhood: string (nullable = true)
    |-- Location: string (nullable = true)
    |-- RowID: string (nullable = true)
    |-- Delay: float (nullable = true)

    """