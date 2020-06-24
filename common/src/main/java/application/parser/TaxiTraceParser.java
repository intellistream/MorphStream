package application.parser;

import application.helper.parser.Parser;
import application.util.datatypes.StreamValues;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class TaxiTraceParser extends Parser {
    //    private static final Logger LOG = LoggerFactory.getLogger(TaxiTraceParser.class);
    private static final DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss");

    private static final int ID_FIELD = 0;
    private static final int NID_FIELD = 1;
    private static final int DATE_FIELD = 2;
    private static final int LAT_FIELD = 3;
    private static final int LON_FIELD = 4;
    private static final int SPEED_FIELD = 5;
    private static final int DIR_FIELD = 6;
    private static final long serialVersionUID = 8825702438518524509L;

    @Override
    public Object[] parse(char[] str) {
        String[] fields = new String(str).split(",");

        if (fields.length != 7) {
            return null;
        }

        try {
            String carId = fields[ID_FIELD];
            DateTime date = formatter.parseDateTime(fields[DATE_FIELD]);
            boolean occ = true;
            double lat = Double.parseDouble(fields[LAT_FIELD]);
            double lon = Double.parseDouble(fields[LON_FIELD]);
            int speed = ((Double) Double.parseDouble(fields[SPEED_FIELD])).intValue();
            int bearing = Integer.parseInt(fields[DIR_FIELD]);
            int msgId = String.format("%s:%s", carId, date.toString()).hashCode();

            Object[] values = new Object[]{carId.toCharArray(), date, occ, speed, bearing, lat, lon};
//            values.setMessageId(msgId);
            return values;//            return ImmutableList.of(values);
        } catch (NumberFormatException ex) {
            System.out.println("Error parsing numeric value" + ex);
            System.exit(-1);
        } catch (IllegalArgumentException ex) {
            System.out.println("Error parsing date/time value" + ex);
            System.exit(-1);
        }

        return null;

//        StreamValues values = new StreamValues("4186589", formatter.parseDateTime("2009-01-06T05:19:07"),
//                true, 0, 0, 40.0426, 116.609, 1);
//        values.setMessageId(0);
//        return values;
    }

    @Override
    public StreamValues parse(String value) {
        return null;
    }
}
