package execution.runtime.tuple.impl;

import common.util.datatypes.StreamValues;
import org.apache.commons.lang.SerializationUtils;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Created by I309939 on 12/7/2016.
 */
class StableValues extends ArrayList<Object> {
    private static final long serialVersionUID = 4056690381745622263L;

    public static StreamValues create(Object... vals) {
        Object[] ret = new Object[vals.length];
        int i = 0;
        for (Object v : vals) {
            ret[i++] = SerializationUtils.clone((Serializable) v);
        }
        return new StreamValues(ret);
    }
}
