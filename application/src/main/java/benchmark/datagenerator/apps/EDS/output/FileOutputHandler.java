package benchmark.datagenerator.apps.EDS.output;

import benchmark.datagenerator.Event;
import benchmark.datagenerator.apps.SL.OCTxnGenerator.SLDataOperationChain;
import benchmark.datagenerator.apps.SL.output.IOutputHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static common.CONTROL.enable_log;

public class FileOutputHandler implements IOutputHandler {
    protected static final Logger log = LoggerFactory.getLogger(FileOutputHandler.class);
    protected String mRootPath;
    protected String fileName;
    protected String mDependencyEdgesFileName;
    protected String mDependencyVerticesFileName;

    public FileOutputHandler(String rootPath) {
        this(rootPath, null, null, null);
    }

    public FileOutputHandler(String rootPath, String fileName, String dependencyFileName, String dependencyVerticesFileName) {
        mRootPath = rootPath;

        if (fileName == null) {
            this.fileName = "events.txt";
        } else
            this.fileName = fileName;

//        this.depositEventFileName = depositEventFileName;
//        if (depositEventFileName == null) {
//            this.depositEventFileName = "depositEvents.txt";
//        }

        mDependencyEdgesFileName = dependencyFileName;
        if (mDependencyEdgesFileName == null) {
            mDependencyEdgesFileName = "dependency_edges.csv";
        }

        mDependencyVerticesFileName = dependencyVerticesFileName;
        if (mDependencyVerticesFileName == null) {
            mDependencyVerticesFileName = "dependency_vertices.csv";
        }

    }


    @Override
    public void sinkEvents(List<Event> events) throws IOException {
        if (enable_log) log.info(String.format("Event file path is %s", mRootPath + fileName));
        BufferedWriter transferEventBufferedWriter = CreateWriter(fileName);
        for (Event event : events) {
            transferEventBufferedWriter.write(event + "\n");
        }
        transferEventBufferedWriter.close();
    }

    @Override
    public void sinkDependenciesEdges(HashMap<Integer, ArrayList<SLDataOperationChain>> allAccountOperationChains, HashMap<Integer, ArrayList<SLDataOperationChain>> allAssetsOperationChains) {

    }

    @Override
    public void sinkDependenciesVertices(HashMap<Integer, ArrayList<SLDataOperationChain>> allAccountOperationChains, HashMap<Integer, ArrayList<SLDataOperationChain>> allAssetsOperationChains) {

    }

    @Override
    public void sinkDependenciesVerticesIdsRange(int accountsRange, int assetsRange) {

    }

    @Override
    public void sinkDistributionOfDependencyLevels() {

    }

    private BufferedWriter CreateWriter(String FileName) throws IOException {
        File file = new File(mRootPath + FileName);
        if (!file.exists())
            file.createNewFile();
        return Files.newBufferedWriter(Paths.get(file.getPath()));
    }


    private void writeDependencyEdges(HashMap<Integer, ArrayList<SLDataOperationChain>> allOperationChains, FileWriter fileWriter) throws IOException {

        for (ArrayList<SLDataOperationChain> operationChains : allOperationChains.values()) {
            for (SLDataOperationChain oc : operationChains) {
                if (!oc.hasChildren()) {
                    ArrayList<String> dependencyChains = oc.getDependencyChainInfo();
                    for (String dependencyChain : dependencyChains) {
                        fileWriter.write("\"" + dependencyChain + "\",\n");
                    }
                }
            }
        }
    }

}
