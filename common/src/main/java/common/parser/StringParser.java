package common.parser;

import common.helper.parser.Parser;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StringParser extends Parser<String, String> {
    private static final Logger LOG = LoggerFactory.getLogger(StringParser.class);
    private static final long serialVersionUID = 3323016460165179360L;

    @Override
    public String parse(String str) {
        if (StringUtils.isBlank(str)) {
            return null;
        }
        return String.copyValueOf(str.toCharArray());
    }
}