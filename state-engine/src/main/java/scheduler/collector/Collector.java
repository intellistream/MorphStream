package scheduler.collector;

import java.util.HashMap;

public class Collector {
    /*
    Num of TD,LD,PD
     */
    private HashMap<Integer,Integer> Num_of_dependency;
    private int B_TD;
    private int B_LD;
    private int B_PD;
    private int B_SUM_D;
    /*
    Vertex Degree Distribution
     */
    private double VDD;
    private double B_VDD;
    /*
    Is Cyclic Dependency
     */
    private boolean isCyclicDependency;
    /*
    Computation Complexity, if high return true
     */
    private boolean isComputationComplexity;
    /*
    Rate of Aborting Vertexes
     */
    private double R_of_A;
    private double B_R_of_A;
    /**
     *  Init the collector after each switch
     */
    private void InitCollector(){
    }

    /**
     * Configure the bottom line for triggering scheduler switching
     */
    public void setBottomLine(){

    }
    /**
     * Implement the decision tree
     * Return the decision of the scheduler switching
     * "OG_BFS","OG_BFS_A","OP_BFS","OP_BFS_A","OG_NS","OG_NS_A","OP_NS","OP_NS_A", "OG_DFS","OG_DFS_A","OP_DFS","OP_DFS_A"
     *  0000     0001       0010     0011       0100    0101      0110    0111       1000      1001      1010     1011
     * @return
     */
    private String decisionTree(int TD,int LD,int PD,double VDD,boolean isCyclicDependency,boolean isComputationComplexity,double R_of_A){
        String schedulers[]={"OG_BFS","OG_BFS_A","OP_BFS","OP_BFS_A","OG_NS","OG_NS_A","OP_NS","OP_NS_A", "OG_DFS","OG_DFS_A","OP_DFS","OP_DFS_A"};
        int flag=0;
        if((TD+PD+LD)>B_SUM_D){
            if(VDD<B_VDD){
                //TODO:Switch the DFS(+8) and BFS(+4)
                flag=flag+4;
            }
        }
        if(!isCyclicDependency&&TD>B_TD&&PD<B_PD){
            flag=flag+2;
        }
        if(!isComputationComplexity&&R_of_A>B_R_of_A){
            flag=flag+1;
        }
        return schedulers[flag];
    }

    public String getDecisionFromConf(){
        return "";
    }

    public String getDecisionFromRuntimeInfo(){
        return decisionTree(Num_of_dependency.get(0),Num_of_dependency.get(1),Num_of_dependency.get(2),VDD,isCyclicDependency,isComputationComplexity,R_of_A);
    }
}
