package java.intellistream.chc.common.dao;

/**
 * Based on the access pattern, the database can use different strategies to update the data.
 */
public enum Strategy {
    PER_FLOW,
    CROSS_FLOW_HEAVY_READ,
    CROSS_FLOW_READ_AND_WRITE,
}
