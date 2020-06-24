package application.bolts.acep;
/*
 * Copyright 2015 Variacode
 *
 * Licensed under the GPLv2 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.gnu.org/licenses/old-licenses/gpl-2.0.txt
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

import sesame.components.context.TopologyContext;
import sesame.components.operators.api.TransactionalBolt;
import sesame.execution.runtime.collector.OutputCollector;
import sesame.execution.runtime.tuple.impl.Fields;
import sesame.execution.runtime.tuple.impl.OutputFieldsDeclarer;
import sesame.execution.runtime.tuple.impl.Tuple;
import com.espertech.esper.client.*;
import com.espertech.esper.client.soda.EPStatementObjectModel;
import state_engine.DatabaseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.*;

/**
 *
 *
 */
public class EsperBolt extends TransactionalBolt implements UpdateListener {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(EsperBolt.class);

    private EPServiceProvider epService;
    private Map<String, List<String>> outputTypes;
    private Map<String, Object> eventTypes;
    private Set<String> statements;
    private Set<EPStatementObjectModel> objectStatements;

    public EsperBolt(int fid) {
        super(LOG, fid);
    }

    public void checkEPLSyntax(String statement) throws EsperBoltException {
        Configuration cepConfig = new Configuration();
        EPServiceProvider service = EPServiceProviderManager.getDefaultProvider(cepConfig);
        service.initialize();
        try {
            service.getEPAdministrator().prepareEPL(statement);
        } catch (EPStatementException e) {
            throw new EsperBoltException(e.getMessage(), e);
        }
        service.destroy();
    }

    /**
     * @param types
     * @return
     */
    public EsperBolt addOutputTypes(Map<String, List<String>> types) {
        String error = "Output Types cannot be null";
        exceptionIfAnyNull(error, types.values());
        for (List<String> s : types.values()) {
            exceptionIfAnyNull(error, s);
        }
        exceptionIfAnyNull(error, types.keySet());
        this.outputTypes = Collections.unmodifiableMap(exceptionIfNull(error, types));
        return this;
    }

    public EsperBolt addEventTypes(Class bean) {
        Map<String, Object> types = new HashMap<>();
        for (Field f : bean.getDeclaredFields()) {
            types.put(f.getName(), f.getType());
        }
        return addEventTypes(types);
    }

    /**
     * @param types
     * @return
     */
    public EsperBolt addEventTypes(Map<String, Object> types) {
        String error = "TxnEvent types cannot be null";
        this.eventTypes = Collections.unmodifiableMap(exceptionIfNull(error, types));
        exceptionIfAnyNull(error, types.values());
        exceptionIfAnyNull(error, types.keySet());
        return this;
    }

    /**
     * @param statements
     * @return
     */
    public EsperBolt addStatements(Set<String> statements) {
        String error = "Statements cannot be null";
        this.statements = Collections.unmodifiableSet(exceptionIfNull(error, statements));
        exceptionIfAnyNull(error, statements);
        return this;
    }

    /**
     * @param objectStatements
     * @return
     */
    public EsperBolt addObjectStatemens(Set<EPStatementObjectModel> objectStatements) {
        final String error = "Object Statements cannot be null";
        this.objectStatements = Collections.unmodifiableSet(exceptionIfNull(error, objectStatements));
        exceptionIfAnyNull(error, objectStatements);
        return this;
    }

    private <O> O exceptionIfNull(String msg, O obj) {
        exceptionIfAnyNull(msg, obj);
        return obj;
    }

    private <O> void exceptionIfAnyNull(String msg, O... obj) {
        for (O o : obj) {
            if (o == null) {
                throw new UnsupportedOperationException(msg);
            }
        }
    }

    /**
     * {@inheritDoc}
     *
     * @param ofd
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
        if (this.outputTypes == null) {
            throw new UnsupportedOperationException("outputTypes cannot be null");
        }
        for (Map.Entry<String, List<String>> outputEventType : this.outputTypes.entrySet()) {
            List<String> fields = new ArrayList<>();
            if (outputEventType.getValue() != null) {
                fields.addAll(outputEventType.getValue());
            } else {
                throw new UnsupportedOperationException();
            }
            ofd.declareStream(outputEventType.getKey(), new Fields(fields));
        }
    }

    /**
     * {@inheritDoc}
     *
     * @param map
     * @param tc
     * @param oc
     */
    @Override
    public void prepare(@SuppressWarnings("rawtypes") Map map, TopologyContext tc, OutputCollector oc) {
        super.prepare(map, tc, oc);
        this.collector = oc;
        Configuration cepConfig = new Configuration();
        if (this.eventTypes == null || (this.objectStatements == null && this.statements == null)) {
            throw new UnsupportedOperationException("TxnEvent types cannot be null and at least one type of statement has to be not null");
        }
        for (Map.Entry a : tc.getThisSources().entrySet()) {
//            Fields f = tc.getComponentOutputFields(a.getKey());
//            if (!this.eventTypes.keySet().containsAll(f.toList())) {
//                throw new UnsupportedOperationException("TxnEvent types and fields from source streams do not match: TxnEvent Types="
//                        + Arrays.toString(this.eventTypes.keySet().toArray())
//                        + " Stream Fields=" + Arrays.toString(f.toList().toArray()));
//            }
//            cepConfig.addEventType(a.getKey().get_componentId() + "_" + a.getKey().get_streamId(), this.eventTypes);
        }
        this.epService = EPServiceProviderManager.getDefaultProvider(cepConfig);
        this.epService.initialize();
        if (!processStatemens()) {
            throw new UnsupportedOperationException("At least one type of statement has to be not empty");
        }
    }

    private boolean processStatemens() {
        boolean hasStatemens = false;
        if (this.statements != null) {
            for (String s : this.statements) {
                this.epService.getEPAdministrator().createEPL(s).addListener(this);
                hasStatemens = true;
            }
        }
        if (this.objectStatements != null) {
            for (EPStatementObjectModel s : this.objectStatements) {
                this.epService.getEPAdministrator().create(s).addListener(this);
                hasStatemens = true;
            }
        }
        return hasStatemens;
    }

    @Override
    protected void TXN_PROCESS(long _bid) throws DatabaseException, InterruptedException {

    }

    /**
     * {@inheritDoc}
     *
     * @param tuple
     */
    @Override
    public void execute(Tuple tuple) {
        Map<String, Object> tuplesper = new HashMap<>();
        for (String f : tuple.getFields()) {
            tuplesper.put(f, tuple.getValueByField(f));
        }
        this.epService.getEPRuntime().sendEvent(tuplesper, tuple.getSourceComponent() + "_" + tuple.getSourceStreamId());
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void cleanup() {
        if (this.epService != null) {
            this.epService.destroy();
        }
    }

    /**
     * {@inheritDoc}
     *
     * @param newEvents
     * @param oldEvents
     */
    @Override
    public void update(EventBean[] newEvents, EventBean[] oldEvents) {
        if (newEvents == null) {
            return;
        }
        for (EventBean newEvent : newEvents) {
            List<Object> tuple = new ArrayList<>();
            String eventType;
            if (outputTypes.containsKey(newEvent.getEventType().getName())) {
                eventType = newEvent.getEventType().getName();
                for (String field : outputTypes.get(newEvent.getEventType().getName())) {
                    if (newEvent.get(field) != null) {
                        tuple.add(newEvent.get(field));
                    }
                }
            } else {
                eventType = "default";
                for (String field : newEvent.getEventType().getPropertyNames()) {
                    tuple.add(newEvent.get(field));
                }
            }
//            collector.emit(eventType, tuple);
        }
    }


    class EsperBoltException extends Exception {

        public EsperBoltException(String msg, Throwable err) {
            super(msg, err);
        }
    }

}
