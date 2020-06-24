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
package application.datatype.toll;

import application.bolts.lr.model.TollEntry;

/**
 * An interface for the storage of toll data (encapsulated in {@link TollEntry}) for historical queries. Allows to Store
 * and retrieve the toll amount.
 *
 * @author richter
 */
public interface TollDataStore {

    /**
     * Retrieves the toll value_list of the underlying data Store. The way the data is retrieved and how efficient and fast
     * this retrieval is depends on implementors.
     *
     * @param xWay              the {@code xWay} field of the query
     * @param day               the {@code day} field of the query
     * @param vehicleIdentifier the {@code vid} field of the query
     * @return the stored toll for the combination of the parameters or {@code null} if there's no such entry
     */
    Integer retrieveToll(int xWay, int day, int vehicleIdentifier);

    /**
     * Stores the toll value_list in the underlying data Store. The way the data is stored and how efficient and fast this
     * storage is depends on implementors.
     *
     * @param xWay              the {@code xWay} field of the query
     * @param day               the {@code day} field of the query
     * @param vehicleIdentifier the {@code vid} field of the query
     * @param toll              the amount to be stored
     */
    /*
     * toll is of type int and not Integer because removing entries from the Store should be done by eliminating the key
     * (in key-value_list-based stores)
     */
    void storeToll(int xWay, int day, int vehicleIdentifier, int toll);

    /**
     * removes the entry associated with {@code xWay}, {@code day} and {@code vehicleIdentifier}
     *
     * @param xWay
     * @param day
     * @param vehicleIdentifier
     * @return
     */
    Integer removeEntry(int xWay, int day, int vehicleIdentifier);
}
