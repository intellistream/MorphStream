package benchmark.datagenerator.apps.SL.TPGTxnGenerator;

import benchmark.datagenerator.DataGenerator;
import benchmark.datagenerator.apps.SL.Transaction.SLDepositEvent;
import benchmark.datagenerator.apps.SL.Transaction.SLEvent;
import benchmark.datagenerator.apps.SL.Transaction.SLTransferEvent;
import common.tools.FastZipfGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

import static common.CONTROL.enable_log;

/**
 * \textbf{Workload Configurations.}
 * We extend SL for workload sensitivity study by tweaking its workload generation for varying dependency characteristics. The default configuration and varying values of parameters are summarized in \tony{Table~\ref{}}.
 * Specifically, we vary the following parameters during workload generation.
 * \begin{enumerate}
 * \item \textbf{Ratio of State Access Types:}
 * We vary the ratio of functional dependencies in the workload by tuning the ratio between transfer (w/ functional dependency) and deposit (w/o functional dependency) requests in the input stream.
 * \item \textbf{State Access Skewness:}
 * To present a more realistic scenario, we model the access distribution as Zipfian skew, where certain states are more likely to be accessed than others. The skewness is controlled by the parameter $\theta$ of the Zipfian distribution. More skewed state access also stands for more temporal dependencies in the workload.
 * \item \textbf{Transaction Length:}
 * We vary the number of operations in the same transaction, which essentially determines the logical dependency depth.
 * \item \textbf{Transaction Aborts:}
 * Transaction will be aborted when balance will become negative. To systematically evaluate the effectiveness of \system in handling transaction aborts, we insert artificial abort in state transactions and vary its ratio in the workload.
 * \end{enumerate}
 */
public class TPGDataGenerator extends DataGenerator {
    private static final Logger LOG = LoggerFactory.getLogger(TPGDataGenerator.class);

    private final int Ratio_Of_Deposit;  // ratio of state access type i.e. deposit or transfer
    private final int State_Access_Skewness; // ratio of state access, following zipf distribution
    private final int Transaction_Length; // transaction length, 4 or 8 or longer
    private final int Ratio_of_Transaction_Aborts; // ratio of transaction aborts, fail the transaction or not. i.e. transfer amount might be invalid.
    // control the number of txns overlap with each other.
    private final ArrayList<Integer> generatedAcc = new ArrayList<>();
    private final ArrayList<Integer> generatedAst = new ArrayList<>();
    // independent transactions.
    private final boolean isUnique = false;
    private final FastZipfGenerator accountZipf;
    private final FastZipfGenerator assetZipf;
    private final Random transactionTypeDecider = new Random(0); // the transaction type decider
    HashMap<Long, Integer> nGeneratedAccountIds = new HashMap<>();
    HashMap<Long, Integer> nGeneratedAssetIds = new HashMap<>();
    private ArrayList<SLEvent> events;
    private int eventID = 0;

    public TPGDataGenerator(TPGDataGeneratorConfig dataConfig) {
        super(dataConfig);

        // TODO: temporarily hard coded, will update later
        Ratio_Of_Deposit = 50;//0-100 (%)
        State_Access_Skewness = 50;
        Transaction_Length = 4;
        Ratio_of_Transaction_Aborts = 0;

        int nKeyState = dataConfig.getnKeyStates();
        events = new ArrayList<>(nTuples);
        // zipf state access generator
        accountZipf = new FastZipfGenerator(nKeyState, (double) State_Access_Skewness / 100, 0);
        assetZipf = new FastZipfGenerator(nKeyState, (double) State_Access_Skewness / 100, 0);
    }

    public static void main(String[] args) {
        FastZipfGenerator fastZipfGenerator = new FastZipfGenerator(10, 1, 0);
        fastZipfGenerator.show_sample();
    }

    protected void generateTuple() {
        SLEvent event;
        int next = transactionTypeDecider.nextInt(100);
        if (next < Ratio_Of_Deposit) {
            event = randomDepositEvent();
        } else {
            event = randomTransferEvent();
        }
        events.add(event);
    }

    private SLEvent randomTransferEvent() {
        // make sure source and destination are different
        int srcAcc, dstAcc, srcAst, dstAst;

        if (!isUnique) {
            srcAcc = accountZipf.next();
            dstAcc = accountZipf.next();
            while (srcAcc == dstAcc) {
                srcAcc = accountZipf.next();
                dstAcc = accountZipf.next();
            }

            srcAst = assetZipf.next();
            dstAst = assetZipf.next();
            while (srcAst == dstAst) {
                srcAst = assetZipf.next();
                dstAst = assetZipf.next();
            }
        } else {
            srcAcc = accountZipf.next();
            while (generatedAcc.contains(srcAcc)) {
                srcAcc = accountZipf.next();
            }
            generatedAcc.add(srcAcc);

            dstAcc = accountZipf.next();
            while (generatedAcc.contains(dstAcc)) {
                dstAcc = accountZipf.next();
            }
            generatedAcc.add(dstAcc);

            srcAst = assetZipf.next();
            while (generatedAst.contains(srcAst)) {
                srcAst = assetZipf.next();
            }
            generatedAst.add(srcAst);

            dstAst = assetZipf.next();
            while (generatedAst.contains(dstAst)) {
                dstAst = assetZipf.next();
            }
            generatedAst.add(dstAst);
        }

        // just for stats record
        nGeneratedAccountIds.put((long) srcAcc, nGeneratedAccountIds.getOrDefault((long) srcAcc, 0) + 1);
        nGeneratedAccountIds.put((long) dstAcc, nGeneratedAccountIds.getOrDefault((long) dstAcc, 0) + 1);
        nGeneratedAssetIds.put((long) srcAst, nGeneratedAccountIds.getOrDefault((long) srcAst, 0) + 1);
        nGeneratedAssetIds.put((long) dstAst, nGeneratedAccountIds.getOrDefault((long) dstAst, 0) + 1);

        SLEvent t = new SLTransferEvent(eventID, srcAcc, srcAst, dstAcc, dstAst);

        // increase the timestamp i.e. transaction id
        eventID++;
        return t;
    }

    private SLEvent randomDepositEvent() {
        int acc = accountZipf.next();
        int ast = assetZipf.next();

        // just for stats record
        nGeneratedAccountIds.put((long) acc, nGeneratedAccountIds.getOrDefault((long) acc, 0) + 1);
        nGeneratedAssetIds.put((long) ast, nGeneratedAccountIds.getOrDefault((long) ast, 0) + 1);

        SLEvent t = new SLDepositEvent(eventID, acc, ast);

        // increase the timestamp i.e. transaction id
        eventID++;
        return t;
    }

    public void dumpGeneratedDataToFile() {
        if (enable_log) LOG.info("++++++" + nGeneratedAccountIds.size());
        if (enable_log) LOG.info("++++++" + nGeneratedAssetIds.size());

        if (enable_log) LOG.info("Dumping transactions...");
        try {
            dataOutputHandler.sinkEvents(events);
        } catch (IOException e) {
            e.printStackTrace();
        }

        File versionFile = new File(dataConfig.getRootPath().substring(0, dataConfig.getRootPath().length() - 1)
                + String.format("_%d.txt", dataConfig.getTotalEvents()));
        try {
            versionFile.createNewFile();
            FileWriter fileWriter = new FileWriter(versionFile);
            fileWriter.write(String.format("Total number of threads  : %d\n", dataConfig.getTotalThreads()));
            fileWriter.write(String.format("Total Events      : %d\n", dataConfig.getTotalEvents()));
            fileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void clearDataStructures() {
        if (events != null) {
            events.clear();
        }
        events = new ArrayList<>();
        // clear the data structure in super class
        super.clearDataStructures();
    }
}
