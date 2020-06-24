package common.model.cdr;
import common.collections.JavaUtils;
import common.util.io.IOUtils;
import common.util.math.RandomUtil;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import org.joda.time.DateTime;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
/**
 * @author maycon
 */
public class CDRDataGenerator {
    public static final int TERMINATION_CAUSE_OK = 0;
    /**
     * http://en.wikipedia.org/wiki/Um_interface
     */
    public static final String[] CALL_TYPES = new String[]{"MOC", "MTC", "SMS-MT", "SMS-MO"};
    private static final Logger LOG = LoggerFactory.getLogger(CDRDataGenerator.class);
    private static final Random rand = new Random();
    private static final String[] RBI = new String[]{"01", "10", "30", "33", "35", "44", "45", "49", "50", "51", "52", "53", "54", "86", "91", "98", "99"};
    /**
     * Codes taken from http://www.cisco.com/en/US/docs/voice_ip_comm/cucm/service/6_0_1/car/carcdrdef.html#wp1062375
     */
    private static final ImmutableMap<Integer, String> TERMINATION_CAUSES = new ImmutableMap.Builder<Integer, String>()
            .put(0, "No error")
            .put(1, "Unallocated (unassigned) number")
            .put(2, "No route to specified transit network (national use)")
            .put(3, "No route to destination")
            .put(4, "Send special information tone")
            .put(5, "Misdialed trunk prefix (national use)")
            .put(6, "Channel unacceptable")
            .put(7, "Call awarded and being delivered in an established channel")
            .put(8, "Preemption")
            .put(9, "Preemption—circuit reserved for reuse")
            .put(16, "Normal call clearing")
            .put(17, "User busy")
            .put(18, "No user responding")
            .put(19, "No answer from user (user alerted)")
            .put(20, "Subscriber absent")
            .put(21, "Call rejected")
            .put(22, "Number changed")
            .put(26, "Non-selected user clearing")
            .put(27, "Destination out of order")
            .put(28, "Invalid number format (address incomplete)")
            .put(29, "Facility rejected")
            .put(30, "Response to STATUS ENQUIRY")
            .put(31, "Normal, unspecified")
            .put(34, "No circuit/channel available")
            .put(38, "Network out of order")
            .put(39, "Permanent frame mode connection out of service")
            .put(40, "Permanent frame mode connection operational")
            .put(41, "Temporary failure")
            .put(42, "Switching equipment congestion")
            .put(43, "Access information discarded")
            .put(44, "Requested circuit/channel not available")
            .put(46, "Precedence call blocked")
            .put(47, "Resource unavailable, unspecified")
            .put(49, "Quality of Service not available")
            .put(50, "Requested facility not subscribed")
            .put(53, "Service operation violated")
            .put(54, "Incoming calls barred")
            .put(55, "Incoming calls barred within Closed User Group (CUG)")
            .put(57, "Bearer capability not authorized")
            .put(58, "Meet-Me secure conference minimum security level not met")
            .put(62, "Inconsistency in designated outgoing access information and subscriber class")
            .put(63, "Service or option not available, unspecified")
            .put(65, "Bearer capability not implemented")
            .put(66, "Channel type not implemented")
            .put(69, "Requested facility not implemented")
            .put(70, "Only restricted digital information bearer capability is available (national use).")
            .put(79, "Service or option not implemented, unspecified")
            .put(81, "Invalid call reference value")
            .put(82, "Identified channel does not exist.")
            .put(83, "A suspended call exists, but this call identity does not.")
            .put(84, "Call identity in use")
            .put(85, "No call suspended")
            .put(86, "Call having the requested call identity has been cleared.")
            .put(87, "User not member of CUG (Closed User Group)")
            .put(88, "Incompatible destination")
            .put(90, "Destination number missing and DC not subscribed")
            .put(91, "Invalid transit network selection (national use)")
            .put(95, "Invalid message, unspecified")
            .put(96, "Mandatory information element is missing.")
            .put(97, "Message type nonexistent or not implemented")
            .put(98, "Message not compatible with the call state, or the message type nonexistent or not implemented")
            .put(99, "An information element or parameter non-existent or not implemented")
            .put(100, "Invalid information element contents")
            .put(101, "Message not compatible with the call state")
            .put(102, "Call terminated when timer expired; a recovery routine executed to recover from the error.")
            .put(103, "Parameter nonexistent or not implemented - passed on (national use)")
            .put(110, "Message with unrecognized parameter discarded")
            .put(111, "Protocol error, unspecified")
            .put(122, "Precedence Level Exceeded")
            .put(123, "Device not Preemptable")
            .put(125, "Out of bandwidth (Cisco specific)")
            .put(126, "Call split (Cisco specific)")
            .put(127, "Interworking, unspecified")
            .put(129, "Precedence out of bandwidth").build();
    public static final Object[] TERMINATION_CAUSE_CODES = TERMINATION_CAUSES.keySet().toArray();
    private static JSONArray countryArray;
    private static Map<String, String> countryMap;
    static {
        try {
            String strJson;
            if (JavaUtils.isJar()) {
                InputStream is = CDRDataGenerator.class.getResourceAsStream("/resources/CountryCodes.json");
                //System.out.println("is:"+is.read());
                assert is != null;
                strJson = IOUtils.convertStreamToString(is);
            } else {
                strJson = Files.toString(new File("src/main/resources/CountryCodes.json"), Charset.defaultCharset());
            }
            //strJson = Files.toString(new File("C://Users//szhang026//Documents//compatibility-applications//src//main//resources//CountryCodes.json"), Charset.defaultCharset());
            countryArray = (JSONArray) new JSONParser().parse(strJson);
            countryMap = new HashMap<>(countryArray.size());
            for (Object obj : countryArray) {
                JSONObject json = (JSONObject) obj;
                countryMap.put((String) json.get("code"), (String) json.get("dial_code"));
            }
        } catch (IOException ex) {
            LOG.error("Error reading country codes file", ex);
            System.exit(-1);
        } catch (ParseException ex) {
            LOG.error("Error parsing country codes file", ex);
            System.exit(-1);
        } catch (Exception ex) {
            System.out.println("Error!!!!" + ex.getMessage());
            System.exit(-1);
        }
    }
    public static String phoneNumber() {
        return phoneNumber("", -1);
    }
    public static String phoneNumber(String countryName, int numDigits) {
        numDigits = (numDigits == -1) ? 11 : numDigits;
        StringBuilder number = new StringBuilder();
        String dialCode;
        if (countryName.isEmpty()) {
            JSONObject country = (JSONObject) countryArray.get(RandomUtil.randInt(0, countryArray.size() - 1));
            dialCode = (String) country.get("dial_code");
        } else {
            dialCode = countryMap.get(countryName);
        }
        number.append(dialCode);
        numDigits -= dialCode.length();
        for (int i = 0; i < numDigits; i++) {
            number.append(RandomUtil.randInt(0, 9));
        }
        return number.toString();
    }
    /**
     * Random IMEI (International Mobile Station Equipment Identity) Number Generator.
     * http://en.wikipedia.org/wiki/IMEI
     * https://github.com/LazyZhu/myblog/blob/gh-pages/imei-generator/js/imei-generator.js
     *
     * @return imei
     * @author LazyZhu (http://lazyzhu.com/)
     */
    public static String imei() {
        int len = 15;
        int len_offset = 0;
        int pos = 0;
        int t = 0;
        int sum = 0;
        int final_digit = 0;
        int[] str = new int[len];
        //
        // Fill in the first two values of the string based with the specified prefix.
        // Reporting Body Identifier list: http://en.wikipedia.org/wiki/Reporting_Body_Identifier
        //
        String[] arr = RBI[(int) Math.floor(Math.random() * RBI.length)].split("");
        str[0] = Integer.parseInt(arr[1]);
        str[1] = Integer.parseInt(arr[2]);
        pos = 2;
        //
        // Fill all the remaining numbers except for the last one with random values.
        //
        while (pos < len - 1) {
            str[pos++] = (int) Math.floor(Math.random() * 10) % 10;
        }
        //
        // Calculate the Luhn checksum of the values thus far.
        //
        len_offset = (len + 1) % 2;
        for (pos = 0; pos < len - 1; pos++) {
            if ((pos + len_offset) % 2 != 0) {
                t = str[pos] * 2;
                if (t > 9) {
                    t -= 9;
                }
                sum += t;
            } else {
                sum += str[pos];
            }
        }
        //
        // Choose the last digit so that it causes the entire string to pass the checksum.
        //
        final_digit = (10 - (sum % 10)) % 10;
        str[len - 1] = final_digit;
        StringBuilder imei = new StringBuilder();
        for (pos = 0; pos < len; pos++)
            imei.append(str[pos]);
        // Output the IMEI value.
        return imei.toString();//.substring(0, len);
    }
    /**
     * Random IMSI (International mobile subscriber identity) generator.
     *
     * @return
     */
    public static String imsi() {
        StringBuilder imsi = new StringBuilder();
        for (int i = 0; i < 15; i++) {
            imsi.append(RandomUtil.randInt(0, 9));
        }
        return imsi.toString();
    }
    public static String callType() {
        return CALL_TYPES[RandomUtil.randInt(0, CALL_TYPES.length - 1)];
    }
    /**
     * Return the code for a random termination cause.
     *
     * @param errorProb The probability of occurrence of an error (all errors have equal chances)
     * @return the code of the termination cause
     */
    public static int causeForTermination(double errorProb) {
        if (rand.nextDouble() < errorProb)
            return (Integer) TERMINATION_CAUSE_CODES[RandomUtil.randInt(1, TERMINATION_CAUSE_CODES.length - 1)];
        else
            return TERMINATION_CAUSE_OK;
    }
    /**
     * Return the information about the cause for termination with the given code.
     *
     * @param code The code of the cause for termination
     * @return The information about the cause for termination code
     */
    public static String causeForTerminationInfo(int code) {
        return TERMINATION_CAUSES.get(code);
    }
    public static String uniqueId() {
        UUID id = UUID.randomUUID();
        return id.toString();
    }
    public static CallDetailRecord createRandom() {
        CallDetailRecord cdr = new CallDetailRecord();
        //cdr.setId(CDRDataGenerator.uniqueId());
        cdr.setCallingNumber(CDRDataGenerator.phoneNumber());
        cdr.setCalledNumber(CDRDataGenerator.phoneNumber());
        cdr.setAnswerTime(DateTime.now());
        cdr.setCallDuration(RandomUtil.randInt(0, 3600 * 24));
        //cdr.setImei(CDRDataGenerator.imei());
        //cdr.setImsi(CDRDataGenerator.imsi());
        //cdr.setBillingPhone(cdr.getCallingNumber());
        //cdr.setRouteEnter(RandomUtil.randInt(0, 100));
        //cdr.setRouteLeft((rand.nextDouble() > 0.5) ? cdr.getRouteEnter() : RandomUtil.randInt(0, 100));
        cdr.setCallEstablished(CDRDataGenerator.causeForTermination(0.05) == TERMINATION_CAUSE_OK);
        return cdr;
    }
}
