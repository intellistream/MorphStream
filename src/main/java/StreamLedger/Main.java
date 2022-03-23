package StreamLedger;

/**
 * Enable transactional stream processing on the shared mutable state.
 * Design decisions:
 * 1. Enable shared mutable state through Redis.
 * 2. Enable consistent shared mutable state access by setting locks on each key.
 * 3. Event will be processed immediately without wait and ordering. Thus, transaction order is not guaranteed.
 */

public class Main {


    public static void main(String[] args) throws Exception {
        // build up Flink pipeline
        StreamLedgerEvaluator streamLedgerEvaluator = new StreamLedgerEvaluator();
        streamLedgerEvaluator.run();
    }
}
