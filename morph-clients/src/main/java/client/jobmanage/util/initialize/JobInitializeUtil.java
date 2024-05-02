package client.jobmanage.util.initialize;

import client.jobmanage.util.Util;
import com.fasterxml.jackson.databind.ObjectMapper;
import dao.Job;
import dao.Operator;
import dao.config.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * JobInitializeUtil is used to initialize a new job
 */
public class JobInitializeUtil {
    private static final Logger log = LoggerFactory.getLogger(JobInitializeUtil.class);

    /**
     * Initialize a new job
     *
     * @param jobName the name of the job
     * @return true if the job is initialized successfully, false otherwise
     */
    public static boolean initialize(String jobName, int parallelism, JobConfiguration jobConfiguration) {
        // create jobInfo directory if not exists
        if (!Util.validateAndMakeDirectory(Util.jobInfoDirectory) || !Util.validateAndMakeDirectory(Util.jobCompileDirectory)) {
            return false;
        }

        int jobId = generateJobId();

        // create job JSON file if not exists
        String jobFileName = String.format("%s/%s.json", Util.jobInfoDirectory, jobId);
        Path jobFile = Paths.get(jobFileName);
        if (Files.exists(jobFile)) {
            log.error("Job JSON file already exists.");
            return false;
        } else {
            try {
                Files.createFile(jobFile);
            } catch (IOException e) {
                log.error("Error in creating Job JSON file: " + e.getMessage());
            }
        }

        File jobInfoFolder = new File(String.format("%s/%s", Util.jobInfoDirectory, jobId));
        Util.validateAndMakeDirectory(jobInfoFolder);

        // create jobCompile directory if not exists
        File jobCompileFolder = new File(String.format("%s/%s", Util.jobCompileDirectory, jobId));
        Util.validateAndMakeDirectory(jobCompileFolder);

        // create jobInfo json file for new job
        ArrayList<Operator> operators = new ArrayList<>();
        for (OperatorDescription operatorDescription : jobConfiguration.getOperatorDescription()) {
            Operator operator = new Operator(String.valueOf(jobConfiguration.getOperatorDescription().indexOf(operatorDescription)), operatorDescription.getName(), parallelism);
            operators.add(operator);
        }

        Job job = new Job(jobId, jobName, parallelism, operators);
        ObjectMapper objectMapper = new ObjectMapper();

        try {
            objectMapper.writeValue(new File(jobFileName), job);
            log.info("Job JSON file created successfully.");
        } catch (IOException e) {
            log.error("Error in creating Job JSON file: " + e.getMessage());
        }
        return true;
    }

    public static String preprocessedCode(String code, JobConfiguration jobConfiguration) {
        // convert string description to a multipart file
        StringBuilder startJobCodeBuilder = new StringBuilder();

        // add necessary imports if not exists
        List<String> necessaryImports = new ArrayList<String>(){{
            add("import client.CliFrontend;");
            add("import intellistream.morphstream.api.Client;");
            add("import intellistream.morphstream.api.state.StateAccess;");
            add("import intellistream.morphstream.api.output.Result;");
            add("import intellistream.morphstream.api.state.StateAccessDescription;");
            add("import intellistream.morphstream.api.state.StateObject;");
            add("import intellistream.morphstream.api.utils.MetaTypes.AccessType;");
            add("import intellistream.morphstream.engine.txn.transaction.TxnDescription;");
            add("import intellistream.morphstream.engine.txn.profiler.RuntimeMonitor;");
            add("import java.util.HashMap;");
            add("import java.util.Objects;");
        }};

        for (String necessaryImport : necessaryImports) {
            if (startJobCodeBuilder.indexOf(necessaryImport) == -1) {
                startJobCodeBuilder.insert(0, necessaryImport + "\n");
            }
        }

        startJobCodeBuilder.append(code);

        // remove the last occurrence of }
        int lastBraceIndex = startJobCodeBuilder.lastIndexOf("}");
        if (lastBraceIndex != -1) {
            startJobCodeBuilder.deleteCharAt(lastBraceIndex);
        }

        startJobCodeBuilder.append("\n");

        startJobCodeBuilder.append("    public static void startJob(String[] args) throws Exception {\n");
        startJobCodeBuilder.append("        CliFrontend job = CliFrontend.getOrCreate().appName(\"")
                .append(jobConfiguration.getName()).append("\");\n");
        startJobCodeBuilder.append("        job.LoadConfiguration(null, args);\n");
        startJobCodeBuilder.append("        job.prepare();\n");
        startJobCodeBuilder.append("        HashMap<String, TxnDescription> txnDescriptions = new HashMap<>();\n");

        for (OperatorDescription opDesc: jobConfiguration.getOperatorDescription()) {
            startJobCodeBuilder.append("        txnDescriptions.clear();\n");
            for (TransactionDescription txnDesc: opDesc.getTransactionDescription()) {
                startJobCodeBuilder.append("        TxnDescription ").append(txnDesc.getName()).append("Descriptor = new TxnDescription();\n");
                for (StateAccessDescription stateAccessDescription : txnDesc.getStateAccessDescription()) {
                    startJobCodeBuilder.append("        StateAccessDescription ").append(stateAccessDescription.getName())
                            .append(" = new StateAccessDescription(\"").append(stateAccessDescription.getName())
                            .append("\", AccessType.").append(stateAccessDescription.getAccessType().toUpperCase()).append(");\n");
                    for (StateObjectDescription stateObjectDescription : stateAccessDescription.getStateObjectDescription()) {
                        startJobCodeBuilder.append("        ").append(stateAccessDescription.getName()).append(".addStateObjectDescription(\"")
                                .append(stateObjectDescription.getName()).append("\", ").append("AccessType.")
                                .append(stateObjectDescription.getAccessType().toUpperCase()).append(", ")
                                .append("\"").append(stateObjectDescription.getTableName()).append("\", ")
                                .append("\"").append(stateObjectDescription.getKeyName()).append("\", ")
                                .append("\"").append(stateObjectDescription.getValueName()).append("\", ")
                                .append(stateObjectDescription.getKeyIndex()).append(");\n");
                    }
                    startJobCodeBuilder.append("        ").append(stateAccessDescription.getName()).append(".addValueName(\"")
                            .append(stateAccessDescription.getValueName()).append("\");\n");
                    startJobCodeBuilder.append("        ").append(txnDesc.getName()).append("Descriptor.addStateAccess(\"")
                            .append(stateAccessDescription.getName()).append("\", ").append(stateAccessDescription.getName()).append(");\n");
                }
                startJobCodeBuilder.append("        txnDescriptions.put(\"").append(txnDesc.getName()).append("\", ")
                        .append(txnDesc.getName()).append("Descriptor);\n\n");
            }
            startJobCodeBuilder.append("        job,setSpoutCombo(opDesc.getName(), txnDescriptions, 4);\n");
            startJobCodeBuilder.append("        RuntimeMonitor.setOperatorIDs(new String[]{opDesc.getName()});\n");
        }

//        startJobCodeBuilder.append("        job.setSpoutCombo(jobConfiguration.getOperatorDescription().getName(), txnDescriptions, 4);\n");
//        startJobCodeBuilder.append("        RuntimeMonitor.setOperatorIDs(new String[]{jobConfiguration.getOperatorDescription().getName()});\n");
        startJobCodeBuilder.append("        try {\n");
        startJobCodeBuilder.append("            job.run();\n");
        startJobCodeBuilder.append("        } catch (InterruptedException ex) {\n");
        startJobCodeBuilder.append("            ex.printStackTrace();\n");
        startJobCodeBuilder.append("        }\n");

        startJobCodeBuilder.append("    }\n");

        startJobCodeBuilder.append("    public static void main(String[] args) throws Exception {\n");
        startJobCodeBuilder.append("        startJob(args);\n");
        startJobCodeBuilder.append("    }\n");
        startJobCodeBuilder.append("}");

        return startJobCodeBuilder.toString();
    }

    /**
     * Concatenate the code
     *
     * @param code       code
     * @param configFile config file
     * @return concatenated code
     */
    public static String preprocessedCode(String code, MultipartFile configFile) {
        String description = "";
        JobConfiguration jobConfiguration = null;
        try {
            description = new String(configFile.getBytes());
            jobConfiguration = new ObjectMapper().readValue(description, JobConfiguration.class);
        } catch (IOException e) {
            log.error("Error in reading description: " + e.getMessage());
        }
        return preprocessedCode(code, jobConfiguration);
    }

    /**
     * Save the code to the file system
     * @param code code
     * @param jobId job id
     * @param jobName job name
     */
    public static void saveCode(String code, String jobId, String jobName) {
        String fileName = String.format("%s/%s/%s.java", Util.jobCompileDirectory, jobId, jobName);
        Path path = Paths.get(fileName);
        if (!Files.exists(path)) {
            try {
                Files.createDirectories(path);
            } catch (IOException e) {
                log.error("Error in creating file: " + e.getMessage());
            }
        }

        try {
            Files.write(path, code.getBytes());
        } catch (IOException e) {
            log.error("Error in saving code: " + e.getMessage());
        }
    }

    /**
     * Generate a new job id
     *
     * @return job id
     */
    private static int generateJobId() {
        if (!Util.jobInfoDirectory.exists() || Util.jobInfoDirectory.listFiles() == null) {
            return 0;   // job id starts from 0
        }

        ArrayList<Integer> existingJobIds = new ArrayList<>();
        for (File file : Objects.requireNonNull(Util.jobInfoDirectory.listFiles())) {
            String fileName = file.getName();
            if (fileName.endsWith(".json")) {
                fileName = fileName.replace(".json", "");
                try {
                    int fileNumber = Integer.parseInt(fileName);
                    existingJobIds.add(fileNumber);
                } catch (NumberFormatException e) {
                    // ignore
                }
            }
        }

        // find the smallest positive integer that is not in the list
        int jobId = 0;
        while (existingJobIds.contains(jobId)) {
            jobId++;
        }
        return jobId;
    }
}
