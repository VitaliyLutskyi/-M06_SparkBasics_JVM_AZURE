import com.byteowls.jopencage.model.JOpenCageLatLng;
import com.epam.data.engineering.model.Hotel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.epam.data.engineering.HotelsWeatherProcessor.enrichHotelsWithLatLng;
import static com.epam.data.engineering.HotelsWeatherProcessor.getLatLng;
import static java.lang.String.valueOf;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class HotelWeatherProcessorTest {

    private static final String CITY = "London";
    private static final double LATITUDE = 42.9814206;
    private static final double LONGITUDE = -81.2465922;

    @Test
    void getLatLngTest() {
        JOpenCageLatLng latLng = getLatLng(CITY);
        assertEquals(LATITUDE, latLng.getLat());
        assertEquals(LONGITUDE, latLng.getLng());
    }

    @Test
    void enrichHotelsWithLatLngTest() {
        Hotel hotel1 = Hotel.builder().city(CITY).latitude(valueOf(LATITUDE)).build();
        Hotel hotel2 = Hotel.builder().city(CITY).longitude(valueOf(LONGITUDE)).build();
        Hotel hotel3 = Hotel.builder().city(CITY).build();

        Hotel hotelExpected = Hotel.builder().city(CITY).longitude(valueOf(LONGITUDE))
                .latitude(valueOf(LATITUDE)).build();

        SparkSession spark = SparkHandlerForTests.getSparkSession();
        Dataset<Hotel> hotels = spark.createDataset(asList(hotel1, hotel2, hotel3), Encoders.bean(Hotel.class));
        List<Hotel> hotelsEnriched = enrichHotelsWithLatLng(hotels).collectAsList();

        assertEquals(asList(hotelExpected, hotelExpected, hotelExpected), hotelsEnriched);
    }
}
