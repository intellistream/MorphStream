package intellistream.morphstream.api.output;

import intellistream.morphstream.api.input.TransactionalEvent;
import lombok.Getter;
import lombok.Setter;


public class Result {
    boolean isLast; //Downstream operator - 0: read transactionalEvent; 1: read result
    @Getter
    TransactionalEvent transactionalEvent;
    //setXXX
    @Getter
    @Setter
    Object[] results;

    public Result() {
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
        if (isLast) {
            StringBuilder stringBuilder = new StringBuilder();
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
