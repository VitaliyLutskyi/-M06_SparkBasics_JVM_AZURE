package com.epam.data.engineering;

import ch.hsr.geohash.GeoHash;
import com.byteowls.jopencage.JOpenCageGeocoder;
import com.byteowls.jopencage.model.JOpenCageForwardRequest;
import com.byteowls.jopencage.model.JOpenCageLatLng;
import com.byteowls.jopencage.model.JOpenCageResponse;
import com.epam.data.engineering.model.Hotel;
import com.epam.data.engineering.model.Weather;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.SaveMode.Overwrite;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.udf;
import static org.apache.spark.sql.types.DataTypes.DoubleType;

@Slf4j
public class HotelsWeatherProcessor {
    private static final String HASH_COL = "hash";
    
    private static final UserDefinedFunction toGeohash = udf((Double latitude, Double longitude) -> GeoHash
            .withCharacterPrecision(latitude, longitude, 4).toBase32(),
            DataTypes.StringType);
    
    public static void process(Dataset<Hotel> hotels, Dataset<Weather> weather) {
        Dataset<Row> hotelsEnriched = enrichHotelsWithLatLng(hotels).withColumn(HASH_COL,
                toGeohash.apply(col("latitude").cast(DoubleType), col("longitude").cast(DoubleType)));

        Dataset<Row> weatherEnriched = weather.withColumn(HASH_COL, toGeohash.apply(col("lat"), col("lng")));

        weatherEnriched.join(hotelsEnriched, weatherEnriched.col(HASH_COL).equalTo(hotelsEnriched.col(HASH_COL)), "left")
                .drop(hotelsEnriched.col(HASH_COL))
                .write()
                .partitionBy("year", "month", "day")
                .mode(Overwrite)
                .parquet("abfss://data@sadevwesteuropevl.dfs.core.windows.net/data");
    }

    public static Dataset<Hotel> enrichHotelsWithLatLng(Dataset<Hotel> hotels) {
        return hotels.map((MapFunction<Hotel, Hotel>)  hotel -> {
            String latitude = hotel.getLatitude();
            String longitude = hotel.getLongitude();
            String city = hotel.getCity();

            if (notAvailable(latitude) || notAvailable(longitude)) {
                JOpenCageLatLng latLng = getLatLng(city);

                if (notAvailable(latitude)) {
                    log.info("City " + city + " latitude = " + latitude);
                    hotel.setLatitude(latLng.getLat().toString());
                }
                if (notAvailable(longitude)) {
                    log.info("City " + city + " longitude = " + longitude);
                    hotel.setLongitude(latLng.getLng().toString());
                }
            }

            return hotel;
        }, Encoders.bean(Hotel.class));
    }

    public static JOpenCageLatLng getLatLng(String city) {
        JOpenCageGeocoder jOpenCageGeocoder = new JOpenCageGeocoder("2cd8c8f430834bb19a321cb53596f555");
        JOpenCageForwardRequest request = new JOpenCageForwardRequest(city);

        JOpenCageResponse response = jOpenCageGeocoder.forward(request);
        return response.getFirstPosition();
    }

    private static boolean notAvailable(String str) {
        return str == null || str.equals("NA");
    }
}
