package application.spout;

import application.constants.BaseConstants;
import application.helper.parser.Parser;
import application.util.OsUtils;
import application.util.datatypes.StreamValues;
import sesame.components.operators.api.AbstractSpout;
import sesame.execution.ExecutionGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;

import static application.Constants.DEFAULT_STREAM_ID;

/**
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class FileSpout extends AbstractSpout {
    private static final Logger LOG = LoggerFactory.getLogger(FileSpout.class);
    private static final long serialVersionUID = 8506350139836095792L;
    protected Parser parser;
    protected File[] files;
    //   private boolean finished = false;
    protected int end_index = 0;//32M
    protected int curFileIndex = 0;
    protected int curLineIndex = 0;
    int loop = 1;
    private Scanner scanner;
    private int cnt = 10240;
    private String file_path;

    private FileSpout() {
        super(LOG);
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        int taskId = getContext().getThisTaskIndex();
        int numTasks = config.getInt(getConfigKey(BaseConstants.BaseConf.SPOUT_THREADS));

        String OS_prefix = null;

        if (OsUtils.isWindows()) {
            OS_prefix = "win.";
        } else {
            OS_prefix = "unix.";
        }
        String path = config.getString(getConfigKey(OS_prefix.concat(BaseConstants.BaseConf.SPOUT_PATH)));
        file_path = System.getProperty("user.home").concat("/Documents/data/app/").concat(path);

        List<String> str_l = new LinkedList<>();

        try {
            openFile(file_path);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void cleanup() {

    }


    @Override
    public void nextTuple() throws InterruptedException {
        if (cnt-- > 0) {//make sure no gc due to queue full.
//            if (input.getMeasure() != null)
//                input.getMeasure().start_measure(loop);
            String value = readFile();
            if (value != null) {
                final StreamValues objects =
                        new StreamValues(value, value);
                collector.emit_bid(DEFAULT_STREAM_ID, objects);
//                if (input.getMeasure() != null)
//                    input.getMeasure().end_measure();
            } else {
                try {
                    openFile(file_path);
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private String readFile() {

        String record = null;

        if (scanner.hasNextLine()) {
            record = readLine();
        }

        return record;
    }

    /**
     * Read one line from the currently open file. If there's only one file, each
     * instance of the spout will read only a portion of the file.
     *
     * @return The line
     */
    private String readLine() {

        //skip the portion of file if it should not be read by current spout thread. -- use in  multi-spout case.
        //      while (scanner.hasNextLine() && ++curLineIndex % numTasks != taskId)
        //          scanner.nextLine();


        if (scanner.hasNextLine())
            return scanner.nextLine();
        else
            return null;
    }

    /**
     * Opens the next file from the index. If there's multiple instances of the
     * spout, it will read only a portion of the files.
     */
    private void openFile(String fileName) throws FileNotFoundException {
        scanner = new Scanner(new File(fileName), "UTF-8");
    }
}	
