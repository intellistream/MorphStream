package application.util.events;

/**
 * Created by szhang026 on 20/2/2016.
 */
public class PositionReportEvent {
    public long time; //A timestamp measured in seconds since the start of the simulation
    public int vid; //vehicle identifier
    public byte speed; // An integer number of miles per hour between 0 and 100
    public byte xWay; //Express way number 0 .. 9
    public byte mile; //Mile number 0..99
    public short offset; // Yards since last Mile Marker 0..1759
    public byte lane; //Travel Lane 0..7. The lanes 0 and 7 are entrance/exit ramps
    public byte dir; //Direction 0(West) or 1 (East)

    public PositionReportEvent() {

    }

    public PositionReportEvent(String[] fields) {
        this.time = Long.parseLong(fields[1]);//Seconds since start of simulation
        this.vid = Integer.parseInt(fields[2]);//Car ID
        this.speed = Byte.parseByte(fields[3]);//An integer number of miles per hour
        this.xWay = Byte.parseByte(fields[4]);//Expressway number
        this.mile = Byte.parseByte(fields[7]);//Mile (This corresponds to the seg field in the original table)
        this.offset = (short) (Integer.parseInt(fields[8]) - (this.mile * 5280)); //Distance from the last mile post
        this.lane = Byte.parseByte(fields[5]); //The lane number
        this.dir = Byte.parseByte(fields[6]); //Direction (west = 0; East = 1)
    }

    //This is special constructor made to support internal protocol based packets. In future this constartur should be merged with the
    //general one which accept only a String[]
    public PositionReportEvent(String[] fields, boolean flg) {
        this.time = Long.parseLong(fields[2]);//Seconds since start of simulation
        this.vid = Integer.parseInt(fields[3]);//Car ID
        this.speed = Byte.parseByte(fields[4]);//An integer number of miles per hour
        this.xWay = Byte.parseByte(fields[5]);//Expressway number
        this.mile = Byte.parseByte(fields[8]);//Mile (This corresponds to the seg field in the original table)
        this.offset = (short) (Integer.parseInt(fields[9]) - (this.mile * 5280)); //Distance from the last mile post
        this.lane = Byte.parseByte(fields[6]); //The lane number
        this.dir = Byte.parseByte(fields[7]); //Direction (west = 0; East = 1)
    }

    public String toString() {
        return "PositionReportEvent -> Time : " + this.time + " vid : " + this.vid + " speed : " + this.speed + " xWay : " + this.xWay + "...";
    }

}
