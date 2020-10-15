package common.topology.transactional.initializer.slinitializer.datagenerator;

public class DataConfig {
    public static int totalBatches = 1;
    public static int tuplesPerBatch = 100000;
    public static float generatedTuplesBeforeAddingDependency = 0.0f; //Percentage of tuples to generate before introducing dependency
    public static float[] dependenciesDistributionToLevels;
    static {
        dependenciesDistributionToLevels = new float[100];
        for(int index=0; index<100; index++) {
            dependenciesDistributionToLevels[index] = 0.01f;
        }
        System.out.println("Demanded distribution...");
        for(int index=0; index<100; index++) {
            System.out.print(String.format("%.2f; ",dependenciesDistributionToLevels[index]));
        }
        System.out.println(dependenciesDistributionToLevels);
    };
}