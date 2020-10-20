package benchmark.datagenerator;

import com.beust.jcommander.Parameter;

public class DatGeneratorConfig {

    @Parameter(names = {"--rootFilePath"}, description = "Root path for data files.")
    public String rootPath = null;

    @Parameter(names = {"--totalEventsPerBatch"}, description = "Total number of events per batch.")
    public int tuplesPerBatch = 10;

    @Parameter(names = {"--numberOfBatches"}, description = "Total number of batches.")
    public int totalBatches = 5;

    @Parameter(names = {"--tuplesBeforeAddingD"}, description = "Tuples to created before starting to add dependencies.")
    public float generatedTuplesBeforeAddingDependency = 0.0f;

    @Parameter(names = {"--numberOfDLevels"}, description = "Number of dependency levels.")
    public int numberOfDLevels = 3;

    @Parameter(names = {"--shufflingActive"}, description = "Number of dependency levels.")
    public boolean shufflingActive = true;

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
        System.out.println(dependenciesDistributionForLevels);
    }

}