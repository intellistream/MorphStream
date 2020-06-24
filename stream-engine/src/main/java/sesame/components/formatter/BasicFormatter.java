package sesame.components.formatter;

import sesame.execution.runtime.tuple.impl.Fields;
import sesame.execution.runtime.tuple.impl.Tuple;

/**
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class BasicFormatter extends Formatter {

    @Override
    public String format(Tuple tuple) {
        Fields schema = context.getComponentOutputFields(tuple.getSourceComponent(), tuple.getSourceStreamId());

        StringBuilder line = new StringBuilder();

        for (int i = 0; i < tuple.fieldSize(); i++) {
            if (i != 0) {
                line.append(", ");
            }
            line.append(String.format("%s=%s", schema.get(i), tuple.getValue(i)));
        }

        return line.toString();
    }

}
