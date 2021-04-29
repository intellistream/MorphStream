package common.model.geoip;
/**
 * @author mayconbordin
 */
public interface IPLocation {
    Location resolve(String ip);
}
