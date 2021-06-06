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

import java.io.Serializable;

/**
 * Each type that contains the three segment identifier attributes XWAY, SEGMENT, DIR must implement this interface.
 *
 * @author mjsax
 */
public interface ISegmentIdentifier extends Serializable {
    Integer getXWay();

    Short getSegment();

    Short getDirection();
}
