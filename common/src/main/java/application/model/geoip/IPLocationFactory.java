package application.model.geoip;


import application.constants.BaseConstants;
import application.util.Configuration;
import application.util.OsUtils;

public class IPLocationFactory {
    public static final String GEOIP2 = "geoip2";

    public static IPLocation create(String name, Configuration config) {
        if (name.equals(GEOIP2)) {
            String OS_prefix = null;
            if (OsUtils.isWindows()) {
                OS_prefix = "win.";
            } else {
                OS_prefix = "unix.";
            }
            return new GeoIP2Location(config.getString(OS_prefix.concat(BaseConstants.BaseConf.GEOIP2_DB)));
        } else {
            throw new IllegalArgumentException(name + " is not a valid IP locator name");
        }
    }
}
