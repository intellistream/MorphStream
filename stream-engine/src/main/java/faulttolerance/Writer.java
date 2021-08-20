package faulttolerance;

import common.collections.OsUtils;
import components.TopologyComponent;
import execution.ExecutionNode;
import net.jpountz.lz4.LZ4BlockOutputStream;
import org.apache.commons.lang3.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import static common.CONTROL.enable_log;

/**
 * Operator shares the same writer.
 * Only one executor needs to perform the write!
 */
public class Writer {
    private final static Logger LOG = LoggerFactory.getLogger(Writer.class);
    public final TopologyComponent operator;
    public final int numTasks;
    int called_executors = 0;
    boolean reliable = true;
    transient MappedByteBuffer out;
    //	private final HashMap<Integer, Collections> collections;
    Collections collections;
    private volatile int iteration = 0;
    private volatile int cnt = 0;

    public Writer(TopologyComponent operator, int numTasks) {
        this.operator = operator;
        this.numTasks = numTasks;
//		this.collections = new HashMap<>();
        this.collections = new Collections(this.operator, this.operator.getNumTasks());
    }

    /**
     * TODO: implement the faster caller to store in future version.
     *
     * @param executor
     */
    public void save_state_MMIO_synchronize(ExecutionNode executor) {
        called_executors++;
        if (called_executors == numTasks) {
            //all has call it.
            if (out != null) {
                out.force();
            }
            reliable = true;
            called_executors = 0;
        }
    }

    private synchronized File create_dir(long msgId) {
        String directory = System.getProperty("user.home")
                + OsUtils.OS_wrapper("TStreamPlus") + OsUtils.OS_wrapper("checkpoints")
                + OsUtils.OS_wrapper(String.valueOf(msgId));
        File file = new File(directory);
        if (!file.mkdirs()) {
            ////LOG.DEBUG("Failed to create the directory to store the snapshots.");
        }
        return file;
    }

    public void save_state_MMIO_compress(long msgId, long timeStampNano
            , int myiteration, String path, boolean compress, ExecutionNode executor, State state) throws IOException {
        if (!reliable) {
            save_state_MMIO_synchronize(executor);
        }
        File file = create_dir(msgId);
        if (compress) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
//			GZIPOutputStream gzipOut = new GZIPOutputStream(baos);
            LZ4BlockOutputStream Lz4out = new LZ4BlockOutputStream(baos);
            ObjectOutputStream objectOut = new ObjectOutputStream(Lz4out);
            objectOut.writeObject(state.value());
            objectOut.close();
            byte[] data = baos.toByteArray();
            //LOG.DEBUG(path + " save state with marker Id:" + msgId + " size to store:" + data.length);
            out = new RandomAccessFile(file
                    + OsUtils.OS_wrapper(path + "@" + timeStampNano), "rw")
                    .getChannel().map(FileChannel.MapMode.READ_WRITE, 0, data.length);
            out.put(data);
        } else {
            byte[] data = SerializationUtils.serialize(state.value());
//			if(enable_log) LOG.info("state size:" + data.length);
            //LOG.DEBUG(path + " save state with marker Id:" + msgId + " size to store:" + data.length);
            out = new RandomAccessFile(file
                    + OsUtils.OS_wrapper(path + "@" + timeStampNano), "rw")
                    .getChannel().map(FileChannel.MapMode.READ_WRITE, 0, data.length);
            out.put(data);
        }
        reliable = false;
//		// decompress data
//		// - method 1: when the decompressed length is known
//		LZ4FastDecompressor decompressor = factory.fastDecompressor();
//		byte[] restored = new byte[decompressedLength];
//		int compressedLength2 = decompressor.decompress(compressed, 0, restored, 0, decompressedLength);
//		// compressedLength == compressedLength2
//
//		// - method 2: when the compressed length is known (a little slower)
//		// the destination buffer needs to be over-sized
//		LZ4SafeDecompressor decompressor2 = factory.safeDecompressor();
//		int decompressedLength2 = decompressor2.decompress(compressed, 0, compressedLength, restored, 0);
//		// decompressedLength == decompressedLength2
    }

    public void save_state(long msgId, long timeStampNano, int myiteration, String path, ExecutionNode executor, State state) throws IOException {
        File file = create_dir(msgId);
        byte[] data = SerializationUtils.serialize(state.value());
        try (FileOutputStream fos = new FileOutputStream(file + OsUtils.OS_wrapper(path + "@" + timeStampNano))) {
            fos.write(data);
            fos.close(); //There is no more need for this line since you had created the instance of "fos" inside the try. And this will automatically close the OutputStream
            //LOG.DEBUG(path + " save state with marker Id:" + msgId + " size to store:" + data.length);
        }
    }

    /**
     * the original MMIO.
     *
     * @param msgId
     * @param timeStampNano
     * @param path
     * @throws IOException
     */
    private void save_state_MMIO(long msgId, Long timeStampNano, String path, ExecutionNode executor, State state) throws IOException {
        //LOG.DEBUG(LOG.getName() + " save state with marker Id:" + msgId);
        if (!reliable && out != null) {
            save_state_MMIO_synchronize(executor);
        }
        File file = create_dir(msgId);
        byte[] data = SerializationUtils.serialize(state.value());
        out = new RandomAccessFile(file
                + OsUtils.OS_wrapper(path + "@" + timeStampNano), "rw")
                .getChannel().map(FileChannel.MapMode.READ_WRITE, 0, data.length);
        out.put(data);
        reliable = false;
    }

    public synchronized void save_state_MMIO_shared(long msgId, long timeStampNano, int myiteration
            , String path, ExecutionNode executor, State state) throws IOException {
//		collections.putIfAbsent(myiteration, new Collections(executor.operator, executor.operator.getNumTasks()));
//		collections.GetAndUpdate(myiteration).add(msgId, timeStampNano, executor, myiteration, state.value_list());
        collections.add(msgId, timeStampNano, executor, myiteration, state.value());
    }

    class Collections {
        final int numTasks;
        private final int base;
        Serializable[] state;//an array of Serializable information from all executors of the same operator.

        Collections(TopologyComponent operator, int numTasks) {
            this.numTasks = numTasks;
            this.base = operator.getExecutorList().get(0).getExecutorID();
        }

        public synchronized void add(long msgId, Long timeStampNano, ExecutionNode executor, int myiteration, Serializable state_value) throws IOException {
            final int index = executor.getExecutorID() - base;
            if (myiteration > iteration) {
                //LOG.DEBUG("Iteration" + myiteration + " started by: " + executor.getOP_full());
                state = new Serializable[numTasks];
                iteration++;
            }
            state[index] = state_value;
            cnt++;
            if (cnt == numTasks) {
                if(enable_log) LOG.info("iteration" + myiteration + " ready to write to disk by: " + executor.getOP_full());
                if (!reliable) {
                    save_state_MMIO_synchronize(executor);
                }
                File file = create_dir(msgId);
//				byte[] data = SerializationUtils.serialize(state);
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
//			GZIPOutputStream gzipOut = new GZIPOutputStream(baos);
                LZ4BlockOutputStream Lz4out = new LZ4BlockOutputStream(baos);
                ObjectOutputStream objectOut = new ObjectOutputStream(Lz4out);
                objectOut.writeObject(state);
                objectOut.close();
                byte[] data = baos.toByteArray();
                if(enable_log) LOG.info("save state with marker Id:" + msgId + " size to store:" + data.length);
                out = new RandomAccessFile(file
                        + OsUtils.OS_wrapper(operator.getId() + "@" + timeStampNano), "rw")
                        .getChannel().map(FileChannel.MapMode.READ_WRITE, 0, data.length);
                out.put(data);
                reliable = false;
                cnt = 0;
            }
        }
    }
}
