package datagenerator;

import com.beust.jcommander.Parameter;
import com.sun.org.apache.xpath.internal.operations.Bool;
import common.collections.OsUtils;

public class DataGeneratorConfig {

    @Parameter(names = {"--totalEventsPerBatch"}, description = "Total number of events per batch.")
    public Integer tuplesPerBatch = 10;

    @Parameter(names = {"--numberOfBatches"}, description = "Total number of batches.")
    public Integer totalBatches = 5;

    @Parameter(names = {"--tuplesBeforeAddingD"}, description = "Tuples to created before starting to add dependencies.")
    public Float generatedTuplesBeforeAddingDependency = 0.0f;

    @Parameter(names = {"--numberOfDLevels"}, description = "Number of dependency levels.")
    public Integer numberOfDLevels = 3;

    @Parameter(names = {"--shufflingActive"}, description = "If transaction should be shuffled before dumping to a file.")
    public Boolean shufflingActive = true;

    @Parameter(names = {"-tt"}, description = "Parallelism for tstream.")
    public Integer totalThreads = 2;

    @Parameter(names = {"--rootFilePath"}, description = "Root path for data files.")
    public String rootPath = System.getProperty("user.home") + OsUtils.OS_wrapper("sesame") + OsUtils.OS_wrapper("SYNTH_DATA");
    public String idsPath = System.getProperty("user.home") + OsUtils.OS_wrapper("sesame") + OsUtils.OS_wrapper("SYNTH_DATA");

    public float[] dependenciesDistributionForLevels;

    public void updateDependencyLevels() {

        dependenciesDistributionForLevels = new float[numberOfDLevels];
        for(int index=0; index<numberOfDLevels; index++) {
            dependenciesDistributionForLevels[index] = 1f / (numberOfDLevels*1.0f);
        }
        System.out.println("Demanded distribution...");
        for(int index=0; index<numberOfDLevels; index++) {
            System.out.print(String.format("%.2f; ",dependenciesDistributionForLevels[index]));
        }
        System.out.println("");
        System.out.println(String.format("totalEventsPerBatch: %d", tuplesPerBatch));
        System.out.println(String.format("numberOfBatches: %d", totalBatches));
        System.out.println(String.format("numberOfDLevels: %d", numberOfDLevels));
        System.out.println(String.format("rootFilePath: %s", rootPath));
    }


}