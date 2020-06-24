package application.parser;

import application.helper.parser.Parser;
import application.util.datatypes.DateUtils;
import application.util.datatypes.StreamValues;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class CommonLogParser extends Parser {
	public static final String IP = "ip";
	public static final String TIMESTAMP = "timestamp";
	public static final String REQUEST = "request";
	public static final String RESPONSE = "response";
	public static final String BYTE_SIZE = "byte_size";
	private static final Logger LOG = LoggerFactory.getLogger(CommonLogParser.class);
	private static final DateTimeFormatter dtFormatter = DateTimeFormat.forPattern("dd/MMM/yyyy:HH:mm:ss Z");
	private static final int NUM_FIELDS = 8;
	private static final long serialVersionUID = -1399317502251028592L;

	public static Map<String, Object> parseLine(String logLine) {
		Map<String, Object> entry = new HashMap<>();
		String logEntryPattern = "^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\S+)(.*?)";


		Pattern p = Pattern.compile(logEntryPattern);
		Matcher matcher = p.matcher(logLine);

		if (!matcher.matches() || NUM_FIELDS != matcher.groupCount()) {
			return null;
		}

		entry.put(IP, matcher.group(1));
		entry.put(TIMESTAMP, dtFormatter.parseDateTime(matcher.group(4)).toDate());
		entry.put(REQUEST, matcher.group(5));
		entry.put(RESPONSE, Integer.parseInt(matcher.group(6)));

		if (matcher.group(7).equals("-")) {
			entry.put(BYTE_SIZE, 0);
		} else {
			entry.put(BYTE_SIZE, Integer.parseInt(matcher.group(7)));
		}

		return entry;
	}

	@Override
	public Object[] parse(char[] str) {
		String s = new String(str);
		Map<String, Object> entry = parseLine(new String(str));

		if (entry == null) {
			LOG.warn("Unable to parse log: {}", s);
			return null;
		}

		long minute = DateUtils.getMinuteForTime((Date) entry.get(TIMESTAMP));
		int msgId = String.format("%s:%s", entry.get(IP), entry.get(TIMESTAMP)).hashCode();

		Object[] items = new Object[]{entry.get(IP), entry.get(TIMESTAMP),
				minute, entry.get(REQUEST), entry.get(RESPONSE), entry.get(BYTE_SIZE)};
//		values.setMessageId(msgId);

//      return ImmutableList.of(values);
		return items;
	}

	@Override
	public StreamValues parse(String value) {
		return null;
	}
}
