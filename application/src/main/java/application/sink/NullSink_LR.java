package application.sink;

import application.datatype.util.LRTopologyControl;
import sesame.components.operators.api.BaseSink;
import sesame.execution.ExecutionGraph;
import sesame.execution.runtime.tuple.JumboTuple;
import sesame.execution.runtime.tuple.impl.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

/**
 * @author mayconbordin
 */
public class NullSink_LR extends BaseSink {
    private static final Logger LOG = LoggerFactory.getLogger(NullSink_LR.class);
    private static final long serialVersionUID = -8603195407227599683L;
    protected ArrayList<String> recorder = new ArrayList<>();
    long start = System.nanoTime();
    private int index_e1;
    private int end_index1 = 100;
    private int index_e2;
    private int end_index2 = 100;
    private transient BufferedWriter writer;
    private long start_true = 0;
    private long end = 0;
    private boolean read1 = true;
    private boolean read2 = true;
    private boolean read3 = true;
    private boolean read4 = true;
    private int index_e3, end_index3 = 100;
    private int index_e4, end_index4 = 100;

    private NullSink_LR() {
        super(LOG);
    }

    //private final int tenM=10000000;

    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        index_e1 = index_e2 = index_e3 = index_e4 = 0;
        start_true = end = 0;
        read1 = read2 = read3 = read4 = true;
    }

    @Override
    public void execute(Tuple in) throws InterruptedException {
        // not in use.
    }

    @Override
    public void execute(JumboTuple in) throws InterruptedException {
        int bound = in.length;
        for (int i = 0; i < bound; i++) {

            switch (in.getSourceStreamId(i)) {
                case LRTopologyControl.TOLL_NOTIFICATIONS_STREAM_ID:
                    index_e1++;
                    if (index_e1 == 1 && read1) {
                        end_index1 = config.getInt("end_index1");
                        LOG.info("end_index1:" + String.valueOf(end_index1));
                        start_true = System.nanoTime();
                        read1 = false;
                    }
                    break;
                case LRTopologyControl.ACCIDENTS_NOIT_STREAM_ID:
                    index_e2++;
                    if (index_e2 == 1 && read2) {
                        end_index2 = config.getInt("end_index2");
                        LOG.info("end_index2:" + String.valueOf(end_index2));
                        read2 = false;
                    }
                    break;
                case LRTopologyControl.ACCOUNT_BALANCE_OUTPUT_STREAM_ID:
                    index_e3++;
                    if (index_e3 == 1 && read3) {
                        end_index3 = config.getInt("end_index3");
                        LOG.info("end_index3:" + String.valueOf(end_index3));
                        read3 = false;
                    }
                    break;
                default:
                    index_e4++;
                    if (index_e4 == 1 && read4) {
                        end_index4 = config.getInt("end_index4");
                        LOG.info("end_index4:" + String.valueOf(end_index4));
                        read4 = false;
                    }
                    break;
            }

            if (in.getMsg(i) != null) {
//            if (index_e1 > 10000)
//                LOG.info("index_e1:" + index_e1);
//            LOG.info("index_e2:" + index_e2);
//            LOG.info("index_e3:" + index_e3);
//            LOG.info("index_e4:" + index_e4);

                if (index_e1 >= end_index1 && index_e2 >= end_index2 && index_e3 >= end_index3 && index_e4 >= end_index4) {//320M
                    end = System.nanoTime();
                    try {
                        FileWriter fw;
                        try {
                            fw = new FileWriter(new File(config.getString("metrics.output") + "/sink.txt"));
                            writer = new BufferedWriter(fw);
                            //writer.write("Received first element\n");
                            //writer.flush();

                        } catch (IOException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }
//        		//System.out.println("Finished execution in:"+((end-start)/1000.0)/1000000.0+" seconds"+"timestamp_counter"+timestamp_counter);
                        //writer.write(start_true+"\n");

//    			for(int i=0;i<recorder.fieldSize();i++){
//    				writer.write(recorder.GetAndUpdate(i)+"\n");
//    			}
                        writer.write(String.valueOf((end - start_true) / 1000000000));
                        //writer.write(((end-start)/1000.0)/1000000.0+"\t"+timestamp_counter+"\n");
                        //writer.write(((end-start_true)/1000.0)/1000000.0+"\t"+timestamp_counter+"\n");
                        writer.flush();
                        writer.close();
//                try {
//					TimeUnit.SECONDS.sleep(5);
//				} catch (InterruptedException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
                    } catch (IOException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                    this.getContext().stop_runningALL();
                }
            }
        }
    }

    @Override
    protected Logger getLogger() {
        return LOG;
    }

}
