package intellistream.morphstream.api.output;

import intellistream.morphstream.api.input.TransactionalEvent;
import lombok.Getter;
import lombok.Setter;


public class Result {
    private long bid;
    boolean isLast; //Downstream operator - 0: read transactionalEvent; 1: read result
    boolean isMeasure= true;
    @Getter
    TransactionalEvent transactionalEvent;
    //setXXX
    @Getter
    @Setter
    Object[] results;

    public Result(long bid) {
        this.bid = bid;
        transactionalEvent = new TransactionalEvent(0);
    }

    public void setBid(int bid) {
        transactionalEvent.setBid(bid);
    }

    public void setLast(boolean last) {
        isLast = last;
    }

    @Override
    public String toString() {
        if (isMeasure) {
            return Long.toString(bid);
        } else if (isLast) {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append(bid);
            stringBuilder.append(",");
            for (Object result : results) {
                stringBuilder.append(result).append(",");
            }
            stringBuilder.deleteCharAt(stringBuilder.length() -1);
            return stringBuilder.toString();
        } else {
            return transactionalEvent.toString();
        }
    }
}
