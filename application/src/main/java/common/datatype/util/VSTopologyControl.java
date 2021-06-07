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

/**
 * TODO
 *
 * @author richter
 * @author mjsax
 */
public class VSTopologyControl {
    // The identifier of the topology.
    public final static String TOPOLOGY_NAME = "VS";
    // spouts
    public final static String SPOUT = "Spout";
    public final static String SINK = "Sink";
    // streams
    // input
    public final static String RCR_STREAM_ID = "RCR";
    public final static String CTBolt_STREAM_ID = "ct";
    public final static String ECR24_STREAM_ID = "ecr24";
    public final static String ECR_STREAM_ID = "ecr";
    public final static String ENCR_STREAM_ID = "encr";
    public final static String URL_STREAM_ID = "url";
    public final static String ACD_STREAM_ID = "acd";
    public final static String FoFIR_STREAM_ID = "FoFIR";
    public final static String GlobalACD_STREAM_ID = "GlobalACD";

    private VSTopologyControl() {
    }

    public interface Field {
    }
}
