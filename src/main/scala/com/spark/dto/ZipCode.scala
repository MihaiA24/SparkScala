package com.spark.dto

case class ZipCode(
                    RecordNumber: Integer,
                    Zipcode: String,
                    ZipCodeType: String,
                    City: String,
                    State: String,
                    LocationType: String,
                    Lat: Double,
                    Long: Double,
                    Yaxis: Double,
                    Xaxis: Double,
                    Zaxis: Double,
                    WorldRegion: String,
                    Country: String,
                    LocationText: String,
                    Location: String,
                    Decommisioned: Boolean
                  )