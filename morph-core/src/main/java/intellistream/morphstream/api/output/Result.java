package intellistream.morphstream.api.output;

import intellistream.morphstream.api.input.TransactionalEvent;


public class Result {
    boolean isLast; //Downstream operator - 0: read transactionalEvent; 1: read result
    TransactionalEvent transactionalEvent;
    Object[] results;

    public Result() {
        transactionalEvent = new TransactionalEvent(0);
    }

    //setXXX
    public void setResults(Object[] results) {
        this.results = results;
    }

    public Object[] getResults() {
        return results;
    }

    public void setBid(int bid) {
        transactionalEvent.setBid(bid);
    }

    public void setLast(boolean last) {
        isLast = last;
    }

    public TransactionalEvent getTransactionalEvent() {
        return transactionalEvent;
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
