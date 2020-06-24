package common.helper;
import common.helper.wrapper.basic.StateWrapper;
import common.collections.ClassLoaderUtils;
import common.util.datatypes.StreamValues;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
/**
 * This is a shared class for both Kafka producer or local producer.
 * Created by I309939 on 8/9/2016.
 */
public class DataSource {
    private static final Logger LOG = LoggerFactory.getLogger(DataSource.class);
    private static int size_elements;
    private static String[] array_key;
    private static long id = 0;
    private final StateWrapper<List<StreamValues>> wrapper;
    protected int counter = 0;
    public DataSource(String parserClass, int skew, boolean test, int tuple_size, boolean verbose) {
        size_elements = tuple_size;
        array_key = new String[size_elements];
        this.wrapper = (StateWrapper<List<StreamValues>>) ClassLoaderUtils.newInstance(parserClass, "wrapper", LOG, tuple_size);
        prepareKey(skew, test);
        counter = 0;
    }
    public DataSource(int skew, boolean test, int tuple_size, boolean verbose) {
        this("ValueStatesWrapper", skew, test, tuple_size, verbose);
    }
    private static void prepareKey(int skew, boolean test) {
        //generate #size_elements keys according to skew
        //FastZipfGenerator z1 = new FastZipfGenerator(size_elements, skew);
        for (int i = 0; i < size_elements; i++) {
            //int k = z1.next();
            array_key[i] = String.valueOf(i);
        }
        if (test)
            System.out.println(Arrays.toString(array_key));
    }
    public Event generateEvent() {
        if (counter == array_key.length)
            counter = 0;
//        int curr=counter;
        Event event = new Event(id++, array_key.length == 0 ? null : array_key[counter++], wrapper.getTuple_states());
//        if(counter!=curr+1){
//            System.exit(-1);
//        }
        return event;
    }
    public Event generateEvent(boolean verbose) {
        if (counter == array_key.length)
            counter = 0;
        return new Event(id++, array_key[counter++], wrapper.getTuple_states(), wrapper.getFlags(counter));
    }
}
