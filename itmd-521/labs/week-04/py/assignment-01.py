from pyspark.sql.types import *
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: assignment-01 <divvy_file>", file=sys.stderr)
        sys.exit(-1)

    # get the divvy data set file name
    divvy_file = sys.argv[1]

    # Create Spark session
    spark = (SparkSession
        .builder
        .appName("DivvyTrips")
        .getOrCreate())

    #create df1 by Infer schema and read divvy csv file
    df1 = spark.read.csv(divvy_file, header=True, inferSchema=True)
    print("======== DF1_Inferred_Schema: Inferred Schema:")
    df1.printSchema()
    df1.show(n=10, truncate=False)
    print("======== DF1_Inferred_Schema Record Count:", df1.count())
 
    #create df2 programmatically using StructFields to create and attach a schema
    schema = StructType([
        StructField("trip_id", IntegerType(), True),
        StructField("starttime", StringType(), True),
        StructField("stoptime", StringType(), True),
        StructField("bikeid", IntegerType(), True),
        StructField("tripduration", IntegerType(), True),
        StructField("from_station_id", IntegerType(), True),
        StructField("from_station_name", StringType(), True),
        StructField("to_station_id", IntegerType(), True),
        StructField("to_station_name", StringType(), True),
        StructField("usertype", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("birthyear", IntegerType(), True),
    ])
    #read divvy csv file into df2
    df2 = spark.read.csv(divvy_file, header=True, schema=schema)
    print("\n======== DF2_Struct_Schema: Schema Created Programmatically using StructFields")
    df2.printSchema()
    print("======== DF2_Struct_Schema Record Count:", df2.count())
    
    #create df3 by ddl. Attach a schema via DDL and read the CSV file
    schema_ddl = "trip_id INT, starttime STRING, stoptime STRING, bikeid INT, tripduration INT, from_station_id INT, from_station_name STRING, to_station_id INT, to_station_name STRING, usertype STRING, gender STRING, birthyear INT"
    # read csv file into df3
    df3 = spark.read.csv(divvy_file, header=True, schema=schema_ddl)
    print("\n======== DF3_DDL_Schema: Schema Attached via DDL")
    df3.printSchema()
    print("======== DF3_DDL_Schema Record Count:", df3.count())


    #Tranformations and actions. Select Gender Female, Groupby to station name
    # For df1
    df1_transf_act = (df1.select("gender", "to_station_name")
	                  .where(df1.gender == 'Female')
                      .groupBy("to_station_name").count())                               
    # show all the resulting aggregation for the df1_transf_act
    df1_transf_act.show(n=10, truncate=False)


    # For df2
    df2_transf_act = (df2.select("gender", "to_station_name")
	                  .where(df2.gender == 'Female')
                      .groupBy("to_station_name").count())                                   
    # show all the resulting aggregation for the df1_transf_act
    df2_transf_act.show(n=10, truncate=False)


    # For df3
    df3_transf_act = (df3.select("gender", "to_station_name")
	                  .where(df3.gender == 'Female')
                      .groupBy("to_station_name").count())                                  
    # show all the resulting aggregation for the df1_transf_act
    df3_transf_act.show(n=10, truncate=False)