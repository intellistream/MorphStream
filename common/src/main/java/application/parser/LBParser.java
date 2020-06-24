package application.parser;

import application.helper.parser.Parser;
import application.util.datatypes.StreamValues;

import java.util.Arrays;

/**
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class LBParser extends Parser {
	private static final long serialVersionUID = 5531330878367909877L;
//    private static final Logger LOG = LoggerFactory.getLogger(StringParser.class);

    @Override
    public Object[] parse(char[] str) {
		if (str.length <= 0) {
			return null;
		}

		return new char[][]{
//				new String(new_copy)
//				sb.toString()
				Arrays.copyOf(str, str.length)
		};//str+""
    }

	@Override
	public StreamValues parse(String value) {
		return null;
	}

}