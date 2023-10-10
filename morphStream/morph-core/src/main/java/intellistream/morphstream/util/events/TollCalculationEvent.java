package intellistream.morphstream.util.events;

/**
 * @author miyuru
 */
public class TollCalculationEvent {
    public int vid; //vehicle identifier
    public int toll; //The toll
    public byte segment; //The mile

    public TollCalculationEvent() {
    }

    public int getVid() {
        return vid;
    }

    public void setVid(int vid) {
        this.vid = vid;
    }

    public int getToll() {
        return toll;
    }

    public void setToll(int toll) {
        this.toll = toll;
    }

    public byte getSegment() {
        return segment;
    }

    public void setSegment(byte segment) {
        this.segment = segment;
    }

    @Override
    public String toString() {
        return "TollCalculationEvent [vid=" + vid + ", toll=" + toll + ", segment=" + segment + "]";
    }

    public String toCompressedString() {
        return vid + " " + toll + " " + segment;
    }
}
