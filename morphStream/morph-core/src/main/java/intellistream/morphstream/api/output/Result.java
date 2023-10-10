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

    public TransactionalEvent getTransactionalEvent() {
        return transactionalEvent;
    }
}
