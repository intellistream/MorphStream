package storage;

public class EventManager {
    public volatile Object[] input_events;
    private int num_events;
    private int insert_num_events = 0;

    public void ini(int num_events) {
        input_events = new Object[num_events];
        this.num_events = num_events;
    }

    public Object get(int bid) {
        if (insert_num_events != num_events) {
            System.out.println("Wrongly insert!");
        }
        if (bid > num_events) {
            System.out.println("Too many bid!" + bid);
        }
        return input_events[bid];
    }

    public void put(Object event, int i) {
        input_events[i] = event;
        insert_num_events++;
    }

    public void clear() {
        input_events = null;
    }
}
