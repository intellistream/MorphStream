package scheduler.collector;

import java.util.HashMap;

public class Collector {
    /*
    Num of TD,LD,PD
     */
    private HashMap<Integer,Integer> Num_of_dependency;
    /*
    Vertex Degree Distribution
     */
    private double VDD;
    /*
    Is Cyclic Dependency
     */
    private boolean IsCyclicDependency;
    /*
    Computation Complexity, if high return true
     */
    private boolean IsComputationComplexity;
    /*
    Rate of Aborting Vertexes
     */
    private double R_of_A;

    /**
     * Implement the decision tree
     * Return the decision of the scheduler switching
     * @return
     */
    public int getDecision(){
        return 0;
    }

    /**
     *  Init the collector after each switch
     */
    private void InitCollector(){

    }

}
