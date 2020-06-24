package common.parser;
import common.helper.parser.Parser;
import common.util.datatypes.StreamValues;
/**
 * Created by tony on 6/20/2017.
 */
public class SAParser extends Parser {
    public static final String split_expression = "\t";
    private static final long serialVersionUID = 7448797732626243378L;
    @Override
    public Object[] parse(char[] input) {
        String[] split = new String(input).split(split_expression);
//        return ImmutableList.of(new StreamValues(Long.valueOf(split[0]), split[1], split[2]));//time, key, value.
        return new Object[]{Long.valueOf(split[0]), split[1].toCharArray(), split[2].toCharArray()};//time, key, value.
    }
    @Override
    public StreamValues parse(String value) {
        return null;
    }
}
