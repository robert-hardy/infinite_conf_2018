on_time_dataframe = spark.read.format('com.databricks.spark.csv')\
    .options(
    header='true',
    treatEmptyValuesAsNulls='true',
    )\
    .load('data/sample.csv')
on_time_dataframe.registerTempTable("on_time_performance")
trimmed_cast_performance = spark.sql("""
    SELECT
      Year, Quarter, Month, DayofMonth, DayOfWeek, FlightDate,
      Carrier, TailNum, FlightNum,
      Origin, OriginCityName, OriginState,
      Dest, DestCityName, DestState,
      DepTime, cast(DepDelay as float), cast(DepDelayMinutes as int),
      cast(TaxiOut as float), cast(TaxiIn as float),
      WheelsOff, WheelsOn,
      ArrTime, cast(ArrDelay as float), cast(ArrDelayMinutes as float),
      cast(Cancelled as int), cast(Diverted as int),
      cast(ActualElapsedTime as float), cast(AirTime as float),
      cast(Flights as int), cast(Distance as float),
      cast(CarrierDelay as float), cast(WeatherDelay as float),
      cast(NASDelay as float),
      cast(SecurityDelay as float),
      cast(LateAircraftDelay as float),
      CRSDepTime, CRSArrTime
    FROM
      on_time_performance
""")
trimmed_cast_performance.registerTempTable("on_time_performance")
trimmed_cast_performance.show()
