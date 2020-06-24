/*
 * #!
 * %
 * Copyright (C) 2014 - 2016 Humboldt-Universität zu Berlin
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
package application.bolts.lr.util;

import application.bolts.lr.TollNotificationBolt;
import application.datatype.PositionReport;
import application.datatype.internal.AccidentTuple;
import application.datatype.internal.CountTuple;
import application.datatype.internal.LavTuple;
import application.datatype.util.LRTopologyControl;
import application.datatype.util.TimeStampExtractor;
import sesame.execution.runtime.tuple.impl.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * {@link TollInputStreamsTsExtractor} helps to merge the four incoming streams of {@link TollNotificationBolt}.
 *
 * @author mjsax
 */
public class TollInputStreamsTsExtractor implements TimeStampExtractor<Tuple> {
    private static final long serialVersionUID = -234551807946550L;
    private static final Logger LOGGER = LoggerFactory.getLogger(TollInputStreamsTsExtractor.class);

    @Override
    public long getTs(Tuple tuple) {


        final String inputStreamId = tuple.getSourceStreamId();
        if (inputStreamId.equals(LRTopologyControl.POSITION_REPORTS_STREAM_ID)) {
            PositionReport report = (PositionReport) tuple.getValue(0);
            return report.getTime();//PositionReport.MIN_IDX.longValue();
        } else if (inputStreamId.equals(LRTopologyControl.ACCIDENTS_STREAM_ID)) {
            return tuple.getShort(AccidentTuple.TIME_IDX).longValue();
        } else if (inputStreamId.equals(LRTopologyControl.CAR_COUNTS_STREAM_ID)) {
            CountTuple report = (CountTuple) tuple.getValue(0);
            return report.getTime();//tuple.getShort(CountTuple.MIN_IDX).longValue();
        } else if (inputStreamId.equals(LRTopologyControl.LAVS_STREAM_ID)) {
            LavTuple report = (LavTuple) tuple.getValue(0);
            return report.getTime();//tuple.getShort(LavTuple.MIN_IDX).longValue() - 60;
        } else {
            LOGGER.error("Unknown input stream: '" + inputStreamId + "' for tuple " + tuple);
            throw new RuntimeException("Unknown input stream: '" + inputStreamId + "' for tuple " + tuple);
        }
    }

}
