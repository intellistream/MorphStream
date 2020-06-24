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
package application.util.lr;

import java.util.Iterator;
import java.util.List;


/**
 * Helper class contains useful methods.
 */
public class Helper {

    public final static String ISSUE_REPORT_URL = "https://github.com/mjsax/aeolus/issues/new";
    /**
     * the key for the
     *  (java.state_engine.utils.Map, TopologyContext, OutputCollector) }
     * method's {@code conf} argument to access the {@code String} to initialize unseralizable instance
     */
    public final static String TOLL_DATA_STORE_CONF_KEY = "myDataStore";

    private Helper() {
    }

    public static String readable(List<String> fields) {
        StringBuilder tmp = new StringBuilder();
        for (Iterator<String> iterator = fields.iterator(); iterator.hasNext(); ) {
            String string = iterator.next();
            if (iterator.hasNext()) {
                tmp.append(string).append("-");
            } else {
                tmp.append(string);
            }
        }
        return tmp.toString();
    }
}
