package java.intellistream.chc.common.dao;

import lombok.Data;

/**
 * Network packet
 */
@Data
public class Packet {
    private final String src;
    private final String dst;
    private final String protocol;
    private final String payload;

    // Other fields
    private final Pattern pattern;
}
