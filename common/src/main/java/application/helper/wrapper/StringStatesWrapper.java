package application.helper.wrapper;

import application.helper.wrapper.basic.StateWrapper;

public class StringStatesWrapper extends StateWrapper {


    private static final long serialVersionUID = -6970166503629636382L;

    public StringStatesWrapper(boolean verbose, int size) {
        super(verbose, size);
    }

    public StringStatesWrapper(int tuple_size) {
        super(tuple_size);
    }
}