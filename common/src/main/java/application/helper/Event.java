package application.helper;

import java.util.LinkedList;

/**
 * TODO: make it generic
 */
public class Event extends LinkedList<String> {
    public static final String split_expression = "\t";
    public static final String null_expression = " ";
	private static final long serialVersionUID = -2725154149946323980L;
	public static int pos_time_stamp = 0;
    public static int pos_key = 1;
    public static int pos_state = 2;

    //    public String value;
//    private String event;
//    private String key;
    public Event() {//this is required by kryo, not sure why..
    }

    public Event(Long timeStamp, String key, String value) {
		String sb = String.valueOf(timeStamp) +
				split_expression +
				key +
				split_expression +
				value;
		//        event = sb.toString();//creates new string here already.
//        this.value = value;
//        this.key = key;
        this.add(String.valueOf(timeStamp));
        this.add(key);
        this.add(value);
        this.add(sb);
    }

    /**
     * TODO: StringBuilder may be slow..
     *
     * @param timeStamp
     * @param key
     * @param value
     * @param flag
     */
    public Event(Long timeStamp, String key, String value, String flag) {
		String sb = String.valueOf(timeStamp) +
				split_expression +
				key +
				split_expression +
				value +
				split_expression +
				flag;
		this.add(String.valueOf(timeStamp));
        this.add(key);
        this.add(value);
        this.add(sb);
        this.add(flag);
    }


    public String getEvent() {
        return this.get(3);
    }

    public String getKey() {
        return this.get(2);
    }
}