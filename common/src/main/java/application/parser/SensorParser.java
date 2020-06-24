package application.parser;

//import applications.state_engine.utils.Configuration;

import application.constants.SpikeDetectionConstants.Conf;
import application.helper.parser.Parser;
import application.util.Configuration;
import application.util.datatypes.StreamValues;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

/**
 * @author mayconbordin
 */
public class SensorParser extends Parser {
	private static final Logger LOG = LoggerFactory.getLogger(SensorParser.class);

	private static final DateTimeFormatter formatterMillis = new DateTimeFormatterBuilder()
			.appendYear(4, 4).appendLiteral("-").appendMonthOfYear(2).appendLiteral("-")
			.appendDayOfMonth(2).appendLiteral(" ").appendHourOfDay(2).appendLiteral(":")
			.appendMinuteOfHour(2).appendLiteral(":").appendSecondOfMinute(2)
			.appendLiteral(".").appendFractionOfSecond(3, 6).toFormatter();

	//  private static final DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");

	private static final int DATE_FIELD = 0;
	private static final int TIME_FIELD = 1;
	private static final int EPOCH_FIELD = 2;
	private static final int MOTEID_FIELD = 3;
	private static final int TEMP_FIELD = 4;
	private static final int HUMID_FIELD = 5;
	private static final int LIGHT_FIELD = 6;
	private static final int VOLT_FIELD = 7;
	private static final ImmutableMap<String, Integer> fieldList = ImmutableMap.<String, Integer>builder()
			.put("temp", TEMP_FIELD)
			.put("humid", HUMID_FIELD)
			.put("light", LIGHT_FIELD)
			.put("volt", VOLT_FIELD)
			.build();
	private static final long serialVersionUID = 7401525897300325924L;
	int deviceId = 0;
	LocalDateTime bkdate = formatterMillis.parseLocalDateTime("2004-02-28 01:54:46.362044");
	private String valueField;
	private int valueFieldKey;

	@Override
	public void initialize(Configuration config) {
		super.initialize(config);

		valueField = config.getString(Conf.PARSER_VALUE_FIELD);
		valueFieldKey = fieldList.get(valueField);
	}

	@Override
	public Object[] parse(char[] input) {
//		String[] fields = new String(input).split("\\s+");
//		ArrayList<char[]> fields = new ArrayList<>();
		char[][] fields = new char[8][];
		int cnt = 0;
		int index = 0;
		int length = input.length;
		int extra_space = 0;
		for (int c = 0; c < length; c++) {
			if (input[c] == ' ' || c == length - 1) {
				if (c + 1 < length && input[c + 1] == ' ') {
					extra_space++;
					continue;
				}
				int len = c - index - extra_space;
				extra_space = 0;
				char[] word = new char[len];
				System.arraycopy(input, index, word, 0, len);
//				collector.emit(word);
//					ll.add(word);
				fields[cnt++] = word;
				index = c + 1;
			}
		}


		/**
		 * 	 0 = "2004-03-31"
		 1 = "03:38:15.757551"
		 2 = "2"
		 3 = "1"
		 4 = "122.153"
		 5 = "-3.91901"
		 6 = "11.04"
		 7 = "2.03397"
		 */

//		String dateStr = String.format("%s %s", new String(fields[DATE_FIELD]), new String(fields[TIME_FIELD]));
//		LocalDateTime date = null;

//		try {
//			date = formatterMillis.parseLocalDateTime(dateStr);
//		} catch (IllegalArgumentException ex) {
//            LOG.warn("Error parsing record date/time field, input record: " + input, ex);
//			System.exit(-1);
//			date = bkdate;
//		}

		try {
			Object[] values = new Object[]{deviceId++, Double.parseDouble(String.valueOf(fields[valueFieldKey]))};

			return values;
		} catch (NumberFormatException ex) {
			System.out.println("Error parsing record numeric field, input record: " + Arrays.toString(input) + ex);
		} catch (Exception anyEx) {
			System.out.println("Something strange happened" + anyEx);
		}
		return null;
	}

	private static final DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");

	@Override
	public List<StreamValues> parse(String input) {
		String[] fields = input.split("\\s+");
		String dateStr = String.format("%s %s", fields[DATE_FIELD], fields[TIME_FIELD]);

		try {
			StreamValues values = new StreamValues();
//			values.add(fields[MOTEID_FIELD]);
			values.add(deviceId++);
//			if (deviceId == 100) {//workaround on device id.
//				deviceId = 0;
//			}
//			values.add(date.toDate());
			values.add(Double.parseDouble(fields[valueFieldKey]));

//			int msgId = String.format("%s:%s", fields[MOTEID_FIELD], date.toString()).hashCode();
//			values.setMessageId(msgId);
			return ImmutableList.of(values);
		} catch (NumberFormatException ex) {
			LOG.info("Error parsing record numeric field, input record: " + input, ex);
		} catch (Exception anyEx) {
			LOG.info("Something strange happened" + anyEx);
		}

		return null;
	}
}
