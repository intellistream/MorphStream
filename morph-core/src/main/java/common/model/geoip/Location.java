package common.model.geoip;

/**
 * @author mayconbordin
 */
public class Location {
    private String countryName;
    private String countryCode;
    private String city;
    private String ip;

    public String getCountryName() {
        return countryName;
    }

    public void setCountryName(String countryName) {
        this.countryName = countryName;
    }

    public String getCountryCode() {
        return countryCode;
    }

    public void setCountryCode(String countryCode) {
        this.countryCode = countryCode;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    @Override
    public String toString() {
        return "Location{" + "countryName=" + countryName + ", countryCode=" + countryCode + ", city=" + city + ", ip=" + ip + '}';
    }
}
