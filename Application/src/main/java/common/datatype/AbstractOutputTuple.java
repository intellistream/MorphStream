/*
 * #!
 * %
 * Copyright (C) 2014 - 2015 Humboldt-Universität zu Berlin
 * %
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #_
 */
package common.datatype;
/**
 * The comm class for all LRB output tuples (including intermediate result tuples).<br />
 * <br />
 * All output tuples do have the following attributes: TYPE, TIME, EMIT
 * <ul>
 * <li>TYPE: the tuple type ID</li>
 * <li>TIME: 'the timestamp of the input tuple that triggered the tuple to be generated' (in LRB seconds)</li>
 * <li>EMIT: 'the timestamp immediately prior to emitting the tuple' (in LRB seconds)</li>
 * </ul>
 *
 * @author mjsax
 */
public abstract class AbstractOutputTuple extends AbstractLRBTuple {
    /**
     * The index of the EMIT attribute.
     */
    private final static int EMIT_IDX = 2;
    // attribute indexes
    private static final long serialVersionUID = 6525749728643815820L;
    protected AbstractOutputTuple() {
        super();
    }
    protected AbstractOutputTuple(Short type, Short time, Short emit) {
        super(type, time);
        assert (emit != null);
        assert (emit.longValue() >= time.longValue());
        super.add(EMIT_IDX, emit);
        assert (super.size() == 3);
    }
    /**
     * Returns the emit timestamp (in LRB seconds) of this {@link AbstractOutputTuple}.
     *
     * @return the emit timestamp of this tuple
     */
    public final Short getEmit() {
        return (Short) super.get(EMIT_IDX);
    }
}
