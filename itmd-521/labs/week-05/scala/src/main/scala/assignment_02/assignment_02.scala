// scalastyle:off println

package main.scala.assignment_02

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
  * Usage: assignment_02 <iot_devices_dataset>
  */
case class DeviceIoTData(
  battery_level: Long, 
  c02_level: Long, 
  cca2: String, 
  cca3: String,
  cn: String,
  device_id: Long,
  device_name: String,
  humidity: Long,
  ip: String,
  latitude: Double,
  lcd: String,
  longitude: Double,
  scale:String,
  temp: Long,
  timestamp: Long
)

object assignment_02 {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("assignment_02")
      .getOrCreate()

    if (args.length < 1) {
      println("Usage: assignment-02 <iot_devices_dataset>")
      sys.exit(1)
    }

    val iot_devices_dataset_path = args(0)

    import spark.implicits._

    val ds = spark.read
      .json(iot_devices_dataset_path)
      .as[DeviceIoTData]

    println("======== iot_devices_Schema: Schema Created using case class")
    ds.printSchema()


    //Questions:

    // 1. Detect failing devices with battery levels below a threshold.
    // Threshold = 3
    ds
    .filter($"battery_level" < 3)
    .select("device_name", "battery_level")
    .show(10, truncate=false)

      /* 
      output:
      +----------------------+-------------+
      |device_name           |battery_level|
      +----------------------+-------------+
      |device-mac-36TWSKiT   |2            |
      |sensor-pad-8xUD6pzsQI |0            |
      |sensor-pad-12Y2kIm0o  |0            |
      |sensor-pad-14QL93sBR0j|1            |
      |meter-gauge-17zb8Fghhl|0            |
      |sensor-pad-36VQv8fnEg |1            |
      |device-mac-39iklYVtvBT|2            |
      |sensor-pad-40NjeMqS   |2            |
      |meter-gauge-43RYonsvaW|2            |
      |sensor-pad-448DeWGL   |0            |
      +----------------------+-------------+
      only showing top 10 rows
      */ 



    // 2. Identify offending countries with high levels of CO2 emissions.
    ds
    .groupBy("cn")
    .agg(sum("c02_level").as("total_c02_level"))
    .orderBy($"total_c02_level".desc)
    .show(10, truncate=false)

      /* 
      output:
      +-----------------+---------------+
      |cn               |total_c02_level|
      +-----------------+---------------+
      |United States    |82270735       |
      |China            |17349538       |
      |Japan            |14479050       |
      |Republic of Korea|14214130       |
      |Germany          |9526623        |
      |United Kingdom   |7799008        |
      |Canada           |7268528        |
      |Russia           |7203677        |
      |France           |6369745        |
      |Brazil           |3896972        |
      +-----------------+---------------+
      only showing top 10 rows
      */



    // 3. Compute the min and max values for temperature, battery level, CO2, and humidity.
    ds
    .select(
      min($"temp").as("min_temp"),
      max($"temp").as("max_temp"),
      min($"battery_level").as("min_battery_level"),
      max($"battery_level").as("max_battery_level"),
      min($"c02_level").as("min_c02_level"),
      max($"c02_level").as("max_c02_level"),
      min($"humidity").as("min_humidity"),
      max($"humidity").as("max_humidity"),
    )
    .show(10, truncate=false)

      /* 
      output:
      +--------+--------+-----------------+-----------------+-------------+-------------+------------+------------+
      |min_temp|max_temp|min_battery_level|max_battery_level|min_c02_level|max_c02_level|min_humidity|max_humidity|
      +--------+--------+-----------------+-----------------+-------------+-------------+------------+------------+
      |10      |34      |0                |9                |800          |1599         |25          |99          |
      +--------+--------+-----------------+-----------------+-------------+-------------+------------+------------+
      */




    // 4. Sort and group by average temperature, CO2, humidity, and country.
    ds
    .groupBy("cn")
    .agg(
      avg("temp").as("avg_temp"),
      avg("c02_level").as("avg_c02_level"),
      avg("humidity").as("avg_humidity"),
    )
    .orderBy("avg_temp", "avg_c02_level", "avg_humidity", "cn")
    .show(10, truncate=false)

      /* 
      output:
      +--------------------------------+------------------+------------------+-----------------+
      |cn                              |avg_temp          |avg_c02_level     |avg_humidity     |
      +--------------------------------+------------------+------------------+-----------------+
      |Vatican City                    |11.0              |1196.0            |35.0             |
      |Samoa                           |12.0              |974.0             |81.0             |
      |Falkland Islands                |13.0              |1424.0            |57.0             |
      |Tonga                           |16.0              |1323.0            |25.0             |
      |Turks and Caicos Islands        |17.0              |862.0             |38.0             |
      |Cook Islands                    |17.333333333333332|1079.0            |85.66666666666667|
      |Gambia                          |17.428571428571427|1240.2857142857142|59.42857142857143|
      |Saint Vincent and the Grenadines|17.454545454545453|1183.0            |57.18181818181818|
      |Guyana                          |17.8              |1069.2            |45.8             |
      |Swaziland                       |17.90909090909091 |1179.8181818181818|70.54545454545455|
      +--------------------------------+------------------+------------------+-----------------+
      only showing top 10 rows
      */


  }
}