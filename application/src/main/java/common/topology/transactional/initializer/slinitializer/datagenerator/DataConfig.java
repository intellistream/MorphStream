package common.topology.transactional.initializer.slinitializer.datagenerator;

public interface DataConfig {
    int totalBatches = 1;
    int tuplesPerBatch = 10000;
    float generatedTuplesBeforeAddingDependency = 0.0f; //Percentage of tuples to generate before introducing dependency
    float[] dependenciesDistributionToLevels = new float[]{0.2f, 0.2f, 0.2f, 0.2f, 0.2f};
}
