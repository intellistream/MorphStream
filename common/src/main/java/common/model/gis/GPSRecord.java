package common.model.gis;

public class GPSRecord extends Point {
    private int vel;
    private int bearing;

    public GPSRecord() {
    }

    public GPSRecord(double x, double y, int vel, int bearing) {
        this.x = x;
        this.y = y;
        this.vel = vel;
        this.bearing = bearing;
    }

    public int getVel() {
        return vel;
    }

    public int getBearing() {
        return bearing;
    }
}
