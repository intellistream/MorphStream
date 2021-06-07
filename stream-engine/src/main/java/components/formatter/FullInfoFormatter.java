package components.formatter;

import execution.runtime.tuple.impl.Fields;
import execution.runtime.tuple.impl.Tuple;

/**
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class FullInfoFormatter extends Formatter {
    private static final String TEMPLATE = "source: %s:%d, stream: %s, id: %s, values: [%s]";

    @Override
    public String format(Tuple tuple) {
        Fields schema = context.getComponentOutputFields(tuple.getSourceComponent(), tuple.getSourceStreamId());
        StringBuilder values = new StringBuilder();
        for (int i = 0; i < tuple.fieldSize(); i++) {
            if (i != 0) {
                values.append(", ");
            }
            values.append(String.format("%s = %s", schema.get(i), tuple.getValue(i)));
        }
        return String.format(TEMPLATE, tuple.getSourceComponent(), tuple.getSourceTask(),
                tuple.getSourceStreamId(), values.toString());
    }
}
