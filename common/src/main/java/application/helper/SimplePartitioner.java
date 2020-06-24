package application.helper;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

public class SimplePartitioner implements Partitioner {
    public SimplePartitioner(VerifiableProperties props) {
    }

    public int partition(Object key, int a_numPartitions) {
//        int partition = 0;
//        String stringKey = (String) key;
//        int offset = stringKey.lastIndexOf('.');
//        if (offset > 0) {
//            partition = Integer.parseInt( stringKey.substring(offset+1)) % a_numPartitions;
//        }
        final int i = Integer.parseInt((String) key) % a_numPartitions;
        //LOG.info(String.valueOf(i));
        return i;
    }

}