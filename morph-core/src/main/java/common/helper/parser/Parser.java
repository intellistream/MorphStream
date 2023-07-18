package common.helper.parser;

import common.collections.Configuration;

import java.io.Serializable;

public abstract class Parser<T, O> implements Serializable {
    private static final long serialVersionUID = -1221926672447206098L;
    protected Configuration config;

    public void initialize(Configuration config) {
        this.config = config;
    }

    public abstract O parse(T value);
}