package intellistream.morphstream.util.events;

/**
 * @author miyuru
 */
public class AccountBalanceEvent {
    public long time; //A timestamp measured in seconds since the start of the simulation
    public int vid; //Vehicle identifier
    public int qid; //Query ID

    public AccountBalanceEvent(String[] fields) {
        this.time = Long.parseLong(fields[1]);//Seconds since start of simulation
        this.vid = Integer.parseInt(fields[2]);//Car ID
        this.qid = Integer.parseInt(fields[9]);//Query ID
    }

    public AccountBalanceEvent(long ttime, int tvid, int tqid) {
        this.time = ttime;//Seconds since start of simulation
        this.vid = tvid;//Car ID
        this.qid = tqid;//Query ID
    }

    @Override
    public String toString() {
        return "AccountBalanceEvent [time=" + time + ", vid=" + vid + ", qid="
                + qid + "]";
    }
}
