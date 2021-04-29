package common.model.gis;
import common.collections.FixedSizeQueue;
public class Road {
    private final int roadID;
    private final FixedSizeQueue<Integer> roadSpeed;
    private int averageSpeed;
    private int count;
    public Road(int roadID) {
        this.roadID = roadID;
        this.roadSpeed = new FixedSizeQueue<>(30);
    }
    public int getRoadID() {
        return roadID;
    }
    public int getAverageSpeed() {
        return averageSpeed;
    }
    public void setAverageSpeed(int averageSpeed) {
        this.averageSpeed = averageSpeed;
    }
    public int getCount() {
        return count;
    }
    public void setCount(int count) {
        this.count = count;
    }
    public void incrementCount() {
        this.count++;
    }
    public FixedSizeQueue<Integer> getRoadSpeed() {
        return roadSpeed;
    }
    public boolean addRoadSpeed(int speed) {
        return roadSpeed.add(speed);
    }
    public int getRoadSpeedSize() {
        return roadSpeed.size();
    }
}
