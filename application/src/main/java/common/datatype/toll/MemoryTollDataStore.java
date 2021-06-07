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
package common.datatype.toll;

import org.apache.commons.lang3.tuple.MutableTriple;
import org.apache.commons.lang3.tuple.Triple;

import java.util.HashMap;
import java.util.Map;

/**
 * a {@link TollDataStore} which stores data in memory. Due to the fact that data to satisfy historical LRB query can
 * GetAndUpdate huge, you're advised to use it carefully, only.<br />
 * <br />
 * This implementation is not thread-safe.
 *
 * @author richter
 */
/*
 * This class is almost used exclusively for tests, but it doesn't hurt to share it to be users started easier.
 */
public class MemoryTollDataStore implements TollDataStore {
    private final Map<Triple<Integer, Integer, Integer>, Integer> store = new HashMap<>();
    private final MutableTriple<Integer, Integer, Integer> reusableMapKey = new MutableTriple<>(
            0, 0, 0);

    // avoid the allocation of memory for every key
    @Override
    public Integer retrieveToll(int xWay, int day, int vehicleIdentifier) {
        this.reusableMapKey.setLeft(xWay);
        this.reusableMapKey.setMiddle(day);
        this.reusableMapKey.setRight(vehicleIdentifier);
        return this.store.get(this.reusableMapKey);
    }

    @Override
    public void storeToll(int xWay, int day, int vehicleIdentifier, int toll) {
        this.reusableMapKey.setLeft(xWay);
        this.reusableMapKey.setMiddle(day);
        this.reusableMapKey.setRight(vehicleIdentifier);
        this.store.put(this.reusableMapKey, toll);
    }

    @Override
    public Integer removeEntry(int xWay, int day, int vehicleIdentifier) {
        this.reusableMapKey.setLeft(xWay);
        this.reusableMapKey.setMiddle(day);
        this.reusableMapKey.setRight(vehicleIdentifier);
        return this.store.remove(this.reusableMapKey);
    }
}
