package common.util.events;

/**
 * Created by szhang026 on 20/2/2016.
 */
public class AccidentEvent {
    public int vid1;//ID of the first vehicle
    public int vid2;
    public byte xway;
    public byte mile;
    public byte dir;
    public long time;

    public AccidentEvent(int vid1, int vid2, byte xway, byte mile, byte dir, long t) {
        this.vid1 = vid1;
        this.vid2 = vid2;
        this.xway = xway;
        this.mile = mile;
        this.dir = dir;
        this.time = t;
    }

    public AccidentEvent() {
    }

    @Override
    public String toString() {
        return "AccidentEvent [vid1 = " + vid1 + ", vid2 = " + vid2 + ", xway=" + xway + ", mile="
                + mile + ", dir=" + dir + " time=" + time + "]";
    }

    public String toCompressedString() {
        return "" + vid1 + " " + vid2 + " " + xway + " " + mile + " " + dir + " " + time;
    }
}
