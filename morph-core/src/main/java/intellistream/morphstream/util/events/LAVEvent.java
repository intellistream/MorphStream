package intellistream.morphstream.util.events;

/**
 * @author miyuru
 * LAV is the Latest Average Velocity in each direction for each one-mile segment. This is computed as
 * the average speeds of the vehicles in each segment, and is computed every minute by averaging the speeds
 * of all position reports issued within the previous 5 minutes.
 */
public class LAVEvent {
    public byte segment; //A segement is in the range 0..99; It corresponds to a mile in the high way system
    public float lav; //Latest Average Velocity
    public byte dir; //Direction of travel (west = 0; East = 1)

    public LAVEvent(byte seg, float velocity, byte dir) {
        this.segment = seg;
        this.lav = velocity;
        this.dir = dir;
    }

    @Override
    public String toString() {
        return "LAVEvent [segment=" + segment + ", lav=" + lav + ", dir=" + dir
                + "]";
    }
}
