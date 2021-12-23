package scheduler.oplevel.struct;

import utils.lib.ConcurrentHashMap;

import java.nio.file.OpenOption;
import java.util.ArrayDeque;
import java.util.ArrayList;

/**
 * There shall be $num_op$ Holders.
 */
public class Holder {
    public ConcurrentHashMap<String, OperationChain> holder_v1 = new ConcurrentHashMap<>();
}
