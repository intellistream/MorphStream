package intellistream.morphstream.api.output;

import intellistream.morphstream.api.input.TransactionalEvent;

import java.util.HashMap;

public class Result {
    boolean isLast; //Downstream operator - 0: read transactionalEvent; 1: read result
    TransactionalEvent transactionalEvent;
    Object[] results;

    public Result() {
        transactionalEvent = new TransactionalEvent(0);
        //TODO: Bolt need to pass bid downstream (for both combo and non-combo cases)
        // Solution 1: Modify the TxnEvent class, make bid as not final, initialize Result in client's post-UDF with bid=0, then bolt modify bid after post-UDF
        // Solution 2: xxx
    }

    //setXXX
    public void setResults(Object[] results) {
        this.results = results;
    }

    public void setKeyMap(HashMap<String, String> keyMap) {
        transactionalEvent.setKeyMap(keyMap);
    }



}
