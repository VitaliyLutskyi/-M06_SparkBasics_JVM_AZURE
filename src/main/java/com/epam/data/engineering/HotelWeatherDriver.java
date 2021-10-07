package com.epam.data.engineering;

import com.epam.data.engineering.model.Hotel;
import com.epam.data.engineering.model.Weather;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

public class HotelWeatherDriver {
    public static void main(String[] args) {
        SparkSession spark = SparkHandler.getSparkSession();

        Dataset<Hotel> hotels = spark.read()
                .option("header", "true")
                .csv("abfss://m06sparkbasics@bd201stacc.dfs.core.windows.net/hotels")
                .as(Encoders.bean(Hotel.class));

        Dataset<Weather> weather = spark.read()
                .parquet("abfss://m06sparkbasics@bd201stacc.dfs.core.windows.net/weather")
                .as(Encoders.bean(Weather.class));

        HotelsWeatherProcessor.process(hotels, weather);

        spark.close();
    }
}
