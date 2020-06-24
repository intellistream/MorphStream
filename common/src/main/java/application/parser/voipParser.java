package application.parser;

import application.helper.parser.Parser;
import application.model.cdr.CallDetailRecord;
import application.util.datatypes.StreamValues;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class voipParser extends Parser {
    private static final Logger LOG = LoggerFactory.getLogger(voipParser.class);
    private static final long serialVersionUID = -7710218185325696134L;

    public voipParser() {

    }

    @Override
    public Object[] parse(char[] input) {
        String str = new String(input);
        if (StringUtils.isBlank(str)) {
            return null;
        }
        CallDetailRecord cdr = buildcdr(str);
        if (cdr == null) {
            LOG.info("Failed to construct cdr:" + str);
            return null;
        }
        return new Object[]{cdr.getCallingNumber(), cdr.getCalledNumber(), cdr.getAnswerTime(), cdr};
    }

    @Override
    public StreamValues parse(String value) {
        return null;
    }


    private CallDetailRecord buildcdr(String str) {
        String[] psb = str.substring(1, str.length() - 1)
                .split(",");
        if (psb.length != 8) {
            return null;
        }
        CallDetailRecord cdr = new CallDetailRecord();

        cdr.setCallingNumber(psb[0].replaceAll(" ", ""));
        cdr.setCalledNumber(psb[1].replaceAll(" ", ""));

        String dateTime = psb[2].replaceAll(" ", "");

        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
        DateTime dt = DateTime.parse(dateTime, formatter);

        cdr.setAnswerTime(dt);
        cdr.setCallDuration(Integer.parseInt(psb[6].split("=")[1].replaceAll(" ", "")));
        cdr.setCallEstablished(Boolean.parseBoolean(psb[7].split("=")[1].replaceAll("}", "")));
        return cdr;
    }
}