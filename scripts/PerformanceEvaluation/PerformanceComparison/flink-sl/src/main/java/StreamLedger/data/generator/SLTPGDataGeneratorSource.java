package StreamLedger.data.generator;

import StreamLedger.data.DepositEvent;
import StreamLedger.data.TransactionEvent;
import StreamLedger.data.tools.FastZipfGenerator;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.types.Either;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A random data generator with data rate throttling logic.
 *
 * <p>This source emits two kinds of events {@link DepositEvent} and a {@link TransactionEvent}. First this source emits
 * deposit events for each account and book entry, and then starts emitting random transaction events while not
 * canceled.
 */
public class SLTPGDataGeneratorSource extends RichParallelSourceFunction<Either<DepositEvent, TransactionEvent>> {

    private static final Logger LOG = LoggerFactory.getLogger(SLTPGDataGeneratorSource.class);
    private static final String ACCOUNT_ID_PREFIX = "ACCT-";
    private static final String BOOK_ENTRY_ID_PREFIX = "BOOK-";
    private final int Ratio_Of_Deposit;  // ratio of state access type i.e. deposit or transfer
    private final int State_Access_Skewness; // ratio of state access, following zipf distribution
    // transaction length, 4 or 8 or longer
    // ratio of transaction aborts, fail the transaction or not. i.e. transfer amount might be invalid.
    private final int Ratio_of_Overlapped_Keys; // ratio of overlapped keys in transactions, which affects the dependencies and circulars.
    // control the number of txns overlap with each other.
    private final ArrayList<Integer> generatedAcc = new ArrayList<>();
    private final ArrayList<Integer> generatedAst = new ArrayList<>();
    // independent transactions.
    private final FastZipfGenerator accountZipf;
    private final FastZipfGenerator assetZipf;
    private final Random random = new Random(0); // the transaction type decider
    private final HashMap<Integer, Integer> nGeneratedAccountIds = new HashMap<>();
    private final HashMap<Integer, Integer> nGeneratedAssetIds = new HashMap<>();
    private final HashMap<Integer, Integer> idToLevel = new HashMap<>();
    private final long nTuples;
    public transient FastZipfGenerator p_generator; // partition generator
    private volatile boolean running = true;

    public SLTPGDataGeneratorSource() {

        // TODO: temporarily hard coded, will update later
        Ratio_Of_Deposit = 25;//0-100 (%)
        State_Access_Skewness = 0;
        Ratio_of_Overlapped_Keys = 0;

        int nKeyState = 122880;

        // allocate levels for each key, to prevent circular.
//        int MAX_LEVEL = (nKeyState / dataConfig.getTotalThreads()) / 2;
        int MAX_LEVEL = 256;
        for (int i = 0; i < nKeyState; i++) {
            idToLevel.put(i, random.nextInt(MAX_LEVEL));
        }

        // zipf state access generator
        accountZipf = new FastZipfGenerator(nKeyState, (double) State_Access_Skewness / 100, 0, 12345678);
        assetZipf = new FastZipfGenerator(nKeyState, (double) State_Access_Skewness / 100, 0, 123456789);
        p_generator = new FastZipfGenerator(nKeyState, (double) State_Access_Skewness / 100, 0);

        nTuples = 24 * 12880;
    }


    @Override
    public void run(SourceContext<Either<DepositEvent, TransactionEvent>> context) throws Exception {
        int eventCount = 0;
        while (running && eventCount < nTuples) {
            int next = random.nextInt(100);
            if (next < Ratio_Of_Deposit) {
                DepositEvent event = randomDepositEvent();
                context.collect(Either.Left(event));
            } else {
                TransactionEvent event = randomTransferEvent();
                context.collect(Either.Right(event));
            }
            eventCount++;
        }
    }

    private TransactionEvent randomTransferEvent() {
        // make sure source and destination are different
        int srcAcc, dstAcc, srcAst, dstAst;

        int[] accKeys = getKeys(accountZipf, generatedAcc);
        srcAcc = accKeys[0];
        dstAcc = accKeys[1];
        int[] astKeys = getKeys(assetZipf, generatedAst);
        srcAst = astKeys[0];
        dstAst = astKeys[1];

        assert srcAcc != dstAcc;
        assert srcAst != dstAst;

        // just for stats record
        nGeneratedAccountIds.put(srcAcc, nGeneratedAccountIds.getOrDefault((long) srcAcc, 0) + 1);
        nGeneratedAccountIds.put(dstAcc, nGeneratedAccountIds.getOrDefault((long) dstAcc, 0) + 1);
        nGeneratedAssetIds.put(srcAst, nGeneratedAccountIds.getOrDefault((long) srcAst, 0) + 1);
        nGeneratedAssetIds.put(dstAst, nGeneratedAccountIds.getOrDefault((long) dstAst, 0) + 1);

        // increase the timestamp i.e. transaction id
        return new TransactionEvent(ACCOUNT_ID_PREFIX + srcAcc,
                BOOK_ENTRY_ID_PREFIX + srcAst,
                ACCOUNT_ID_PREFIX + dstAcc,
                BOOK_ENTRY_ID_PREFIX + dstAst,
                200, 200, 200);
    }

    private DepositEvent randomDepositEvent() {
        int acc = getKey(accountZipf, generatedAcc);
        int ast = getKey(assetZipf, generatedAst);

        // just for stats record
        nGeneratedAccountIds.put(acc, nGeneratedAccountIds.getOrDefault((long) acc, 0) + 1);
        nGeneratedAssetIds.put(ast, nGeneratedAccountIds.getOrDefault((long) ast, 0) + 1);

        DepositEvent t = new DepositEvent(ACCOUNT_ID_PREFIX + acc, BOOK_ENTRY_ID_PREFIX + ast, 200L, 200L);

        // increase the timestamp i.e. transaction id
        return t;
    }

    private int[] getKeys(FastZipfGenerator zipfGenerator, ArrayList<Integer> generatedKeys) {
        int[] keys = new int[2];
        keys[0] = getKey(zipfGenerator, generatedKeys);
        keys[1] = getKey(zipfGenerator, generatedKeys);

        while (keys[0] == keys[1] || idToLevel.get(keys[0]) >= idToLevel.get(keys[1])) {
            keys[0] = getKey(zipfGenerator, generatedKeys);
            keys[1] = getKey(zipfGenerator, generatedKeys);
        }

        generatedKeys.add(keys[0]);
        generatedKeys.add(keys[1]);
        return keys;
    }

    private int getKey(FastZipfGenerator zipfGenerator, ArrayList<Integer> generatedKeys) {
        int srcKey;
        srcKey = zipfGenerator.next();
        int next = random.nextInt(100);
        if (next < Ratio_of_Overlapped_Keys) { // randomly select a key from existing keyset.
            if (!generatedKeys.isEmpty()) {
                srcKey = generatedKeys.get(zipfGenerator.next() % generatedKeys.size());
            }
        }
        return srcKey;
    }

    @Override
    public void cancel() {
        running = false;
    }
}
