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
package common.datatype.util;

import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.Tuple;

import java.io.Serializable;

/**
 * {@link TimeStampExtractor} extract the timestamp from a given {@link Tuple} or {@link Values}. (Type {@code T} is
 * expected to be either {@link Tuple} (for usage in bolts) or {@link Values} (for usage in spouts).)
 *
 * @author mjsax
 */
public interface TimeStampExtractor<T> extends Serializable {
    long getTs(T tuple);
}
