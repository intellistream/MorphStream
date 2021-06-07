package faulttolerance.impl;

import faulttolerance.State;

import java.io.Serializable;

/**
 * Single value_list State
 */
public class ValueState<E extends Serializable> extends State<E> {
    private static final long serialVersionUID = 5982252287746705128L;
    E value;

    public ValueState() {
    }

    /**
     * Updates the operator state accessible by ValueState.value_list() to the given value_list.
     *
     * @param value
     */
    @Override
    public void update(E value) {
        this.value = value;
    }

    @Override
    public E value() {
        return value;
    }

    @Override
    public void clean() {
        value = null;
    }
}
