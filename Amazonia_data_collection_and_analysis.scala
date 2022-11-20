#1: for the animal tracking, we can use the first method


val schema = "TrackerId INT, Species STRING, humidity Long, temp Long, timestamp Long, latitude Double, longitude Double, districtnumber INT"
val tracker = spark.read.schema(schema).json("/user/hackathon_data.json")
ds. printSchema
tracker.show()
ds. show(5, false)

#count all Eira barbara records
tracker.select ("TrackerID", "Species", "timestemp") .where ($"Species" === EiraBarbara).count()

# track single animal with trackerId = 2437
tracker.select ( "timestamp", "latitude", "longitude") .where ($"TrackerId" === 2437).show()

# track MazanaAmericana records in district number = 2436 humidity = 0.89 orderBy timestamp
tracker.select("TrackerID", "Species", "timestemp"). where($"Species" === MazanaAmericana and "districtnumber" === 2436 and humidity = 0.89) . orderBy (desc ("timestamp")).show()


#2: Agouti are small are hard to place traker, so we woule like to put trackers on trees as well to acknowledge deforestation.
case class TreeTracker(
  battery_level: Long,
  co2_level: Long,
  device_id: Long,
  device_name: String,
  humidity: Long,
  species: String,
  ip: String,
  latitude: Double,
  longitude: Double,
  districtnumber: Int,
  temp: Long,
  timestamp: Long
)
val ds = spark.read.json("/user/treedata.json") -as [DeviceIoTData]
ds. printSchema
ds. show(5, false)

#We can do some operations like this:
#Show 5 records that run out out battery. 
ds.select ("battery_level", "longitude", "latitude") .where ($"battery_level" === 0).show(5, false)

# count Tree type: BrazilNut records with 600 ppm co2_level
ds.select ("battery_level", "longitude", "latitude") .where ($"species" === BrazilNut and "co2_level" === 600).count
