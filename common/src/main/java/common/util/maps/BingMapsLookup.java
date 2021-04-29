package common.util.maps;
import com.google.common.base.Optional;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.Map;
public final class BingMapsLookup {
    private static final Logger LOG = LoggerFactory.getLogger(BingMapsLookup.class);
    private static final String BING_MAPS_URL_START = "http://dev.virtualearth.net/REST/v1/Locations/";
    private static final String BING_MAPS_URL_MIDDLE_JSON = "?o=json&key=";
    private static String BING_MAPS_API_KEY;
    public static void setApiKey(String apiKey) {
        BING_MAPS_API_KEY = apiKey;
    }
    public final static Optional<String> reverseGeocodeFromLatLong(final double latitude, final double longitude) {
        final StringBuilder bingMapsURL = new StringBuilder();
        bingMapsURL.append(BING_MAPS_URL_START)
                .append(latitude)
                .append(",")
                .append(longitude)
                .append(BING_MAPS_URL_MIDDLE_JSON)
                .append(BING_MAPS_API_KEY);
        //LOG.DEBUG("BingMapsURL==>{}", bingMapsURL.toString());
        HttpURLConnection httpURLConnection;
        InputStream inputStream = null;
        try {
            final URL url = new URL(bingMapsURL.toString());
            httpURLConnection = (HttpURLConnection) url.openConnection();
            if (HttpURLConnection.HTTP_OK == httpURLConnection.getResponseCode()) {
                inputStream = httpURLConnection.getInputStream();
                return getStateFromJSONResponse(inputStream);
            }
        } catch (final Throwable throwable) {
            LOG.error(throwable.getMessage(), throwable);
            throwable.printStackTrace();
        } finally {
            if (null != inputStream) {
                try {
                    inputStream.close();
                } catch (final IOException ioException) {
                    LOG.error(ioException.getMessage(), ioException);
                    ioException.printStackTrace();
                }
            }
            httpURLConnection = null;
        }
        return Optional.absent();
    }
    @SuppressWarnings("unchecked")
    private static Optional<String> getStateFromJSONResponse(InputStream inputStream) {
        final ObjectMapper mapper = new ObjectMapper();
        try {
            //final Map<String,Object> bingResponse = (Map<String, Object>) mapper.readValue(new File("C:/BingMaps_JSON_Response.json"), Map.class);
            final Map<String, Object> bingResponse = (Map<String, Object>) mapper.readValue(inputStream, Map.class);
            if (200 == Integer.parseInt(String.valueOf(bingResponse.get("statusCode")))) {
                final List<Map<String, Object>> resourceSets = (List<Map<String, Object>>) bingResponse.get("resourceSets");
                if (resourceSets != null && resourceSets.size() > 0) {
                    final List<Map<String, Object>> resources = (List<Map<String, Object>>) resourceSets.get(0).get("resources");
                    if (resources != null && resources.size() > 0) {
                        final Map<String, Object> address = (Map<String, Object>) resources.get(0).get("address");
                        //LOG.DEBUG("State==>{}", address.get("adminDistrict"));
                        return Optional.of((String) address.get("adminDistrict"));
                    }
                }
            }
        } catch (final IOException ioException) {
            LOG.error(ioException.getMessage(), ioException);
            ioException.printStackTrace();
        }
        return Optional.absent();
    }
}