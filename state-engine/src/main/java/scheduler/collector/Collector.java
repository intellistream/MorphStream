package scheduler.collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
/**
 * Gather information for decision trees and determine when to switch and which scheduler to switch to
 * Created by curry on 3/10/2022.
 */
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
    private boolean isRuntime=true;
    private int threadCount;
    private final List<String[]> workloadConfig=new ArrayList<>();
    private final HashMap<Integer,Integer> currentWorkload=new HashMap<>();
    /**
     *  Init the collector
     */
    public void InitCollector(int threadCount){
        this.threadCount=threadCount;
        for (int i=0;i<threadCount;i++){
            currentWorkload.put(i,0);
        }
    }
    /**
     * Configure the bottom line for triggering scheduler switching
     */
    public void setBottomLine(String bottomLine){
        String b_ls[]=bottomLine.split(",");
        this.B_TD= Integer.parseInt(b_ls[0]);
        this.B_LD= Integer.parseInt(b_ls[1]);
        this.B_PD= Integer.parseInt(b_ls[2]);
        this.B_SUM_D=Integer.parseInt(b_ls[3]);
        this.B_VDD= Double.parseDouble(b_ls[4]);
        this.B_R_of_A= Double.parseDouble(b_ls[5]);
    }
    /**
     * Load the workload config if not collecting information runtime
     */
    public void setWorkloadConfig(String config){
        isRuntime=false;
        String configs[]=config.split(";");
        for (String c:configs){
           workloadConfig.add(c.split(","));
        }
    }
    /**
     * Implement the decision tree
     * Return the decision of the scheduler switching
     * Abort(Eager:1,Lazy:0)(first bit)
     * Units(OP:1,OG:0)(second bit)
     * Exploration(Non:00,BF:01,DF:10)(the highest bit)
     * "OG_NS","OG_NS_A","OP_NS","OP_NS_A","OG_BFS","OG_BFS_A","OP_BFS","OP_BFS_A", "OG_DFS","OG_DFS_A","OP_DFS","OP_DFS_A"
     *  0000     0001     0010     0011     0100     0101       0110     0111        1000     1001       1010     1011
     * @return
     */
    private String decisionTree(int TD,int LD,int PD,double VDD,boolean isCyclicDependency,boolean isComputationComplexity,double R_of_A){
        String schedulers[]={"OG_NS","OG_NS_A","OP_NS","OP_NS_A","OG_BFS","OG_BFS_A","OP_BFS","OP_BFS_A","OG_DFS","OG_DFS_A","OP_DFS","OP_DFS_A"};
        int flag=0;
        if((TD+PD+LD)>B_SUM_D){
            if(VDD<B_VDD){
                //TODO:Switch the DFS(+8) and BFS(+4)
                flag=flag+4;
            }
        }
        if(isCyclicDependency||TD<B_TD||PD>B_PD){
            flag=flag+2;
        }
        if(isComputationComplexity||R_of_A<B_R_of_A){
            flag=flag+1;
        }
        return schedulers[flag];
    }

    private String getDecisionFromConf(int threadId){
        int workloadId=currentWorkload.get(threadId);
        String para[]=workloadConfig.get(workloadId);
        int TD= Integer.parseInt(para[0]);
        int LD= Integer.parseInt(para[1]);
        int PD= Integer.parseInt(para[2]);
        double VDD=Double.parseDouble(para[3]);
        double R_of_A=Double.parseDouble(para[4]);
        boolean isCD = false,isCC=false;
        if(para[5].equals("1")){
            isCD=true;
        }
        if(para[6].equals("1")){
            isCC=true;
        }
        return decisionTree(TD,LD,PD,VDD,isCD,isCC,R_of_A);
    }

    private String getDecisionFromRuntimeInfo(){
        return decisionTree(Num_of_dependency.get(0),Num_of_dependency.get(1),Num_of_dependency.get(2),VDD,isCyclicDependency,isComputationComplexity,R_of_A);
    }
    public String getDecision(int threadId){
        if(isRuntime){
            return getDecisionFromRuntimeInfo();
        }else {
            return getDecisionFromConf(threadId);
        }
    }

    public boolean timeToSwitch(long markId,int threadId){
        if(isRuntime){
            //TODO:collect information runtime
            return false;
        }else{
            int workloadId=this.currentWorkload.get(threadId);
            long currentId= Long.parseLong(workloadConfig.get(workloadId)[workloadConfig.get(workloadId).length-1]);
            if (currentId<=markId+threadCount-threadId-1){
                currentWorkload.put(threadId,currentWorkload.get(threadId)+1);
                return true;
            }else
                return false;
        }
    }
}
