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
 * The comm class for all LRB input tuples (ie, position reports and query requests).<br />
 * <br />
 * All input tuples do have the following attributes: TYPE, TIME, VID
 * <ul>
 * <li>TYPE: the tuple type ID</li>
 * <li>TIME: 'the timestamp of the input tuple that triggered the tuple to be generated' (in LRB seconds)</li>
 * <li>VID: the unique vehicle ID</li>
 * </ul>
 *
 * @author mjsax
 */
public abstract class AbstractInputTuple extends AbstractLRBTuple {
    /**
     * The index of the VID attribute.
     */
    public final static int VID_IDX = 2;
    // attribute indexes
    private final static long serialVersionUID = 5607968457968961059L;
    protected AbstractInputTuple() {
        super();
    }
    protected AbstractInputTuple(Short type, Short time, Integer vid) {
        super(type, time);
        assert (vid != null);
        super.add(VID_IDX, vid);
        assert (super.size() == 3);
    }
    /**
     * Returns the vehicle ID of this {@link AbstractInputTuple}.
     *
     * @return the VID of this tuple
     */
    public final Integer getVid() {
        return (Integer) super.get(VID_IDX);
    }
}
