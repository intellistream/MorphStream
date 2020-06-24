package application.param;

import state_engine.storage.SchemaRecordRef;

import java.util.Random;
import java.util.Set;

import static application.constants.PositionKeepingConstants.Constant.SIZE_EVENT;

public class PKEvent {
    static final Random r = new Random();
    private final Set<Integer> key;//single key input_event, work on each device.
    private double[][] value;//Note, it should be arraylist instead of linkedlist as there's no add/remove later.


    private final SchemaRecordRef[] mean_value_ref;//this is essentially the place-holder..
    private final SchemaRecordRef[] list_value_ref;//this is essentially the place-holder..

    public double[] enqueue_time = new double[1];
    //    public double[] useful_ratio = new double[1];
    public double[] index_time = new double[1];
    public int sum;
    long bid;//input_event sequence, shall be set by input_event sequencer.
    long emit_timestamp = 0;

    public PKEvent(long bid, Set<Integer> deviceID, double[][] value) {
        this.bid = bid;
        this.key = deviceID;

        mean_value_ref = new SchemaRecordRef[SIZE_EVENT];
        list_value_ref = new SchemaRecordRef[SIZE_EVENT];

        for (int i = 0; i < SIZE_EVENT; i++) {
            mean_value_ref[i] = new SchemaRecordRef();
            list_value_ref[i] = new SchemaRecordRef();
        }
        this.value = value;
    }


    public Set<Integer> getKey() {
        return key;
    }

    public double[] getValue(int i) {
        return value[i];
    }

    public SchemaRecordRef getMean_value_ref(int i) {
        return mean_value_ref[i];
    }

    public SchemaRecordRef getList_value_ref(int i) {
        return list_value_ref[i];
    }


    public long getEmit_timestamp() {
        return emit_timestamp;
    }

    public long getBid() {
        return bid;//act as bid..
    }

    public void setBid(long bid) {
        this.bid = bid;
    }


}