package sesame.optimization.impl;
import sesame.execution.ExecutionNode;
public class Decision {
    public final ExecutionNode producer;
    public final ExecutionNode consumer;
    public Decision(ExecutionNode producer, ExecutionNode consumer) {
        this.producer = producer;
        this.consumer = consumer;
    }
}
