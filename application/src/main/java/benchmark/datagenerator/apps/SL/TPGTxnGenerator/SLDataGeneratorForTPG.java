package benchmark.datagenerator.apps.SL.TPGTxnGenerator;

import benchmark.datagenerator.DataGenerator;
import benchmark.datagenerator.DataGeneratorConfig;
import benchmark.datagenerator.SpecialDataGenerator;
import benchmark.datagenerator.apps.SL.OCTxnGenerator.SLDataTransaction;
import common.tools.FastZipfGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

/**
 * \textbf{Workload Configurations.}
 * We extend SL for workload sensitivity study by tweaking its workload generation for varying dependency characteristics. The default configuration and varying values of parameters are summarized in \tony{Table~\ref{}}.
 * Specifically, we vary the following parameters during workload generation.
 * \begin{enumerate}
 *     \item \textbf{Ratio of State Access Types:}
 *     We vary the ratio of functional dependencies in the workload by tuning the ratio between transfer (w/ functional dependency) and deposit (w/o functional dependency) requests in the input stream.
 *     \item \textbf{State Access Skewness:}
 *     To present a more realistic scenario, we model the access distribution as Zipfian skew, where certain states are more likely to be accessed than others. The skewness is controlled by the parameter $\theta$ of the Zipfian distribution. More skewed state access also stands for more temporal dependencies in the workload.
 *     \item \textbf{Transaction Length:}
 *     We vary the number of operations in the same transaction, which essentially determines the logical dependency depth.
 *     \item \textbf{Transaction Aborts:}
 *     Transaction will be aborted when balance will become negative. To systematically evaluate the effectiveness of \system in handling transaction aborts, we insert artificial abort in state transactions and vary its ratio in the workload.
 * \end{enumerate}
 */
public class SLDataGeneratorForTPG extends SpecialDataGenerator {
    private static final Logger LOG = LoggerFactory.getLogger(SLDataGeneratorForTPG.class);

    private final int Ratio_Of_Deposit;  // ratio of state access type i.e. deposit or transfer
    private final int State_Access_Skewness; // ratio of state access, following zipf distribution
    private final int Transaction_Length; // transaction length, 4 or 8 or longer
    private final int Ratio_of_Transaction_Aborts; // ratio of transaction aborts, fail the transaction or not. i.e. transfer amount might be invalid.

    protected final int nTotalTransactionsToGenerate;
    private ArrayList<SLDataTransaction> dataTransactions;

    // control the number of txns overlap with each other.
    private final ArrayList<Integer> generatedAcc = new ArrayList<>();
    private final ArrayList<Integer> generatedAst = new ArrayList<>();
    // independent transactions.
    private final boolean isUnique = false;

    HashMap<Long, Integer> nGeneratedAccountIds = new HashMap<>();
    HashMap<Long, Integer> nGeneratedAssetIds = new HashMap<>();

    private final FastZipfGenerator accountZipf;
    private final FastZipfGenerator assetZipf;
    private final Random transactionTypeDecider = new Random(0); // the transaction type decider

    private int transactionId = 0;

    public SLDataGeneratorForTPG(DataGeneratorConfigForTPG dataConfig) {
        super(dataConfig);

        // TODO: temporarily hard coded, will update later
        Ratio_Of_Deposit = 0;
        State_Access_Skewness = 1;
        Transaction_Length = 4;
        Ratio_of_Transaction_Aborts = 0;


        int nKeyState = dataConfig.nKeyState;
        nTotalTransactionsToGenerate = mTotalTuplesToGenerate;
        dataTransactions = new ArrayList<>(nTotalTransactionsToGenerate);
        // zipf state access generator
        accountZipf = new FastZipfGenerator(nKeyState, (double) State_Access_Skewness/100, 0);
        assetZipf = new FastZipfGenerator(nKeyState, (double) State_Access_Skewness/100, 0);
    }

    @Override
    public void generateStream() {
        // if file is already exist, skip generation
        if (isFileExist())
            return;

        for (int tupleNumber = 0; tupleNumber < nTotalTransactionsToGenerate; tupleNumber++) {
            // by far only generate 1/10 tuples and replicate 10 times when dumping outside
            generateTuple();
        }

        LOG.info(String.format("Data Generator will dump data at %s.", dataConfig.rootPath));
        dumpGeneratedDataToFile();
        LOG.info("Data Generation is done...");
        clearDataStructures();
    }

    protected void generateTuple() {
//         select keys in zipf distribution
        SLTransaction t;
        int next = transactionTypeDecider.nextInt(100);
        if (next < Ratio_Of_Deposit) {
            t = randomDepositEvent();
        } else {
            t = randomTransferEvent();
        }
        dataTransactions.add(t);
    }

    private SLTransaction randomTransferEvent() {
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

        SLTransferTransaction t = new SLTransferTransaction(transactionId, srcAcc, srcAst, dstAcc, dstAst);

        // increase the timestamp i.e. transaction id
        transactionId++;
        return t;
    }

    private SLTransaction randomDepositEvent() {
        int acc = accountZipf.next();
        int ast = assetZipf.next();

        // just for stats record
        nGeneratedAccountIds.put((long) acc, nGeneratedAccountIds.getOrDefault((long) acc, 0) + 1);
        nGeneratedAssetIds.put((long) ast, nGeneratedAccountIds.getOrDefault((long) ast, 0) + 1);

        SLDepositTransaction t = new SLDepositTransaction(transactionId, acc, ast);

        // increase the timestamp i.e. transaction id
        transactionId++;
        return t;
    }


    protected void dumpGeneratedDataToFile() {
//        System.out.println("++++++" + nGeneratedAccountIds);
        System.out.println("++++++" + nGeneratedAccountIds.size());
        System.out.println("++++++" + nGeneratedAssetIds.size());

        File file = new File(dataConfig.rootPath);
        if (file.exists()) {
            LOG.info("Data already exists.. skipping data generation...");
            return;
        }
        file.mkdirs();

        File versionFile = new File(dataConfig.rootPath.substring(0, dataConfig.rootPath.length() - 1)
                + String.format("_%d_%d.txt", dataConfig.tuplesPerBatch, dataConfig.totalBatches));
        try {
            versionFile.createNewFile();
            FileWriter fileWriter = new FileWriter(versionFile);
            fileWriter.write(String.format("Tuples per batch      : %d\n", dataConfig.tuplesPerBatch));
            fileWriter.write(String.format("Total batches         : %d\n", dataConfig.totalBatches));
            fileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        LOG.info("Dumping transactions...");
        mDataOutputHandler.sinkTransactionsNoReplication(dataTransactions);
        LOG.info("Dumping Dependency Vertices ids range...");
        mDataOutputHandler.sinkDependenciesVerticesIdsRange(nGeneratedAssetIds.size(), nGeneratedAccountIds.size());
    }

    @Override
    protected void clearDataStructures() {
        if (dataTransactions != null) {
            dataTransactions.clear();
        }
        dataTransactions = new ArrayList<>();
        // clear the data structure in super class
        super.clearDataStructures();
    }

    public static void main(String[] args) {
        FastZipfGenerator fastZipfGenerator = new FastZipfGenerator(10, 1, 0);
        fastZipfGenerator.show_sample();
    }
}
