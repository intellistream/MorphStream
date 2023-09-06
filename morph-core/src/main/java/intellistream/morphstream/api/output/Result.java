package intellistream.morphstream.api.output;

import intellistream.morphstream.api.input.TransactionalEvent;

import java.util.HashMap;

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

    public void setKeyMap(HashMap<String, String> keyMap) {

    }

    public void setValueMap(HashMap<String, Object> valueMap) {

    }



}
