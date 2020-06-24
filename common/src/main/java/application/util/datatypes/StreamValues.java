package application.util.datatypes;


import java.util.ArrayList;

public class StreamValues<E> extends ArrayList<E> {
	private static final long serialVersionUID = 7498957996924776844L;
	private Object messageId;
	private String streamId = "default";

//	public ImmutableList<E> getValue() {
//		return value;
//	}

	//	private ImmutableList<E> value;
//public E get(int idx) {
//		return value.get(idx);
//	}
	public StreamValues() {
	}

	public StreamValues(E... vals) {

//		super(vals.length);
//		this.addAll(ImmutableList.copyOf(vals));
//		value = ImmutableList.copyOf(vals);

		super(vals.length);
		for(E o: vals) {
			add(o);
		}
	}

	public Object getMessageId() {
		return messageId;
	}

	public void setMessageId(Object messageId) {
		this.messageId = messageId;
	}

	public String getStreamId() {
		return streamId;
	}

	public void setStreamId(String streamId) {
		this.streamId = streamId;
	}

//
}
