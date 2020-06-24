package application.parser;

import application.helper.parser.Parser;
import application.util.datatypes.StreamValues;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

/**
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class StringParser extends Parser<char[]> {
	private static final Logger LOG = LoggerFactory.getLogger(StringParser.class);
	private static final long serialVersionUID = 3323016460165179360L;
//    private static final Logger LOG = LoggerFactory.getLogger(StringParser.class);

	@Override
	public char[] parse(char[] str) {
		if (str.length <= 0) {
			return null;
		}

//		StringBuffer sb = new StringBuffer(str);
//		sb.append(" ");
//		LOG.info("Address of Char:" + OsUtils.Addresser.addressOf(sb.toString().charAt(0)));

//		char[] new_copy = new char[str.length()];
//		str.getChars(0, str.length(), new_copy, 0);
//		return new char[][]{
////				new String(new_copy)
////				sb.toString()
//				Arrays.copyOf(str, str.length)
//		};//str+""
		//new StringBuilder().append(str).append("").toString()};//a workaround to ensure parser has populated a new string.
//		char[] copyOf = Arrays.copyOf(str, str.length);
		return Arrays.copyOf(str, str.length);///Arrays.copyOf(str, str.length);//cross memory copy?
		// SerializationUtils.clone(str);  512 cycles
		// new String(str).toCharArray() 372.1536 cycles.
		// Arrays.copyOf(str, str.length) 345 - 512.688
	}

	/**
	 * The original parser used in Storm and Flink
	 *
	 * @return
	 */
	@Override
	public List<StreamValues> parse(String str) {
		if (StringUtils.isBlank(str)) {
			return null;
		}

		return ImmutableList.of(new StreamValues(str));
	}

//
//    @Override
//    public List<StreamValues> parse(String[] input) {
//        List<String> srt = new LinkedList<>();
//
//        for (int i = 0; i < input.length; i++) {
//            if (input[i] != null)
//                srt.add(input[i]);
//        }
//        StreamValues rt = new StreamValues(srt);
//        return ImmutableList.of(rt);
//    }
}