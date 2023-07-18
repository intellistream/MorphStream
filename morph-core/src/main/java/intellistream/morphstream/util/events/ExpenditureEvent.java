package intellistream.morphstream.util.events;

/**
 * @author miyuru
 */
public class ExpenditureEvent {
    public long time; //A timestamp measured in seconds since the start of the simulation
    public int vid; //vehicle identifier
    public int qid; //Query ID
    public byte xWay; //Express way number 0 .. 9
    public int day; //The day for which the daily expenditure value is needed

    public ExpenditureEvent(String[] fields) {
        this.time = Long.parseLong(fields[1]);//Seconds since start of simulation
        this.vid = Integer.parseInt(fields[2]);//Car ID
        this.qid = Integer.parseInt(fields[9]);//Query ID
        this.xWay = Byte.parseByte(fields[4]);//Expressway number
        this.day = Integer.parseInt(fields[14]);//Day
    }

    public ExpenditureEvent() {
    }

    @Override
    public String toString() {
        return "ExpenditureEvent [time=" + time + ", vid=" + vid + ", qid="
                + qid + ", xWay=" + xWay + "]";
    }
}
