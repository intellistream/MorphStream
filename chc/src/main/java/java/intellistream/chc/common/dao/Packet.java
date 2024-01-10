package java.intellistream.chc.common.dao;

import lombok.Data;

/**
 * Network packet
 */
@Data
public class Packet {
    public final String src;
    public final String dst;
    public final String protocol;
    public final String payload;
}
