package intellistream.morphstream.api.input;

import intellistream.morphstream.common.io.ByteIO.SyncFileAppender;
import intellistream.morphstream.common.io.Encoding.decoder.*;
import intellistream.morphstream.common.io.Encoding.encoder.*;
import intellistream.morphstream.common.io.Utils.Binary;
import intellistream.morphstream.configuration.Configuration;
import intellistream.morphstream.engine.txn.TxnEvent;
import intellistream.morphstream.engine.txn.durability.inputStore.InputDurabilityHelper;
import intellistream.morphstream.engine.txn.durability.struct.FaultToleranceRelax;
import intellistream.morphstream.engine.txn.profiler.MeasureTools;
import intellistream.morphstream.util.FaultToleranceConstants;
import intellistream.morphstream.util.OsUtils;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static java.nio.file.StandardOpenOption.READ;

/**
 * protected Encoder typeEncoder;//int
 * protected Encoder timestampEncoder;//long
 * protected Encoder bidEncoder;//long
 * protected Encoder v1Encoder;//sourceAccountId(int)
 * protected Encoder v2Encoder;//sourceAssetId(int)
 * protected Encoder v3Encoder;//accountValue(long)
 * protected Encoder v4Encoder;//bookEntryValue(long)
 * protected Encoder v5Encoder;//destinationAccountId(int)
 * protected Encoder v6Encoder;//destinationAssetId(int)
 */
public class ApplicationInputDurabilityHelper extends InputDurabilityHelper {
    public ByteArrayOutputStream baos = new ByteArrayOutputStream();
    protected Encoder timestampEncoder;
    protected Decoder timestampDecoder;
    protected Encoder longEncoder;
    protected Decoder[] longDecoders = new Decoder[3];
    protected Encoder intEncoder;
    protected Decoder[] intDecoders = new Decoder[5];
    protected Encoder stringEncoder;
    protected Decoder stringDecoder;
    //Whether encode input in an entire string
    protected boolean isStringEncoded = false;


    public ApplicationInputDurabilityHelper(Configuration configuration, int taskId, FaultToleranceConstants.CompressionType compressionType) {
        this.tthread = configuration.getInt("tthread");
        this.partitionOffset = configuration.getInt("NUM_ITEMS") / tthread;
        this.encodingType = compressionType;
        this.ftOption = configuration.getInt("FTOption");
        this.taskId = taskId;
        switch (compressionType) {
            case None:
                this.isCompression = false;
                break;
            case XOR:
                this.timestampEncoder = new LongGorillaEncoder();
                this.timestampDecoder = new LongGorillaDecoder();
                this.longEncoder = new LongGorillaEncoder();
                for (int i = 0; i < longDecoders.length; i++) {
                    longDecoders[i] = new LongGorillaDecoder();
                }
                this.intEncoder = new IntGorillaEncoder();
                for (int i = 0; i < intDecoders.length; i++) {
                    intDecoders[i] = new IntGorillaDecoder();
                }
                break;
            case Delta2Delta:
                this.timestampEncoder = new DoublePrecisionEncoderV1();
                this.longEncoder = new SinglePrecisionEncoderV1();
                break;
            case Delta:
                this.timestampEncoder = new DeltaBinaryEncoder.LongDeltaEncoder();
                this.timestampDecoder = new DeltaBinaryDecoder.LongDeltaDecoder();
                this.longEncoder = new DeltaBinaryEncoder.LongDeltaEncoder();
                for (int i = 0; i < longDecoders.length; i++) {
                    longDecoders[i] = new DeltaBinaryDecoder.LongDeltaDecoder();
                }
                this.intEncoder = new DeltaBinaryEncoder.IntDeltaEncoder();
                for (int i = 0; i < intDecoders.length; i++) {
                    intDecoders[i] = new DeltaBinaryDecoder.IntDeltaDecoder();
                }
                break;
            case RLE:
                this.timestampEncoder = new LongRleEncoder();
                this.timestampDecoder = new LongRleDecoder();
                this.longEncoder = new LongRleEncoder();
                for (int i = 0; i < longDecoders.length; i++) {
                    longDecoders[i] = new LongRleDecoder();
                }
                this.intEncoder = new IntRleEncoder();
                for (int i = 0; i < intDecoders.length; i++) {
                    intDecoders[i] = new IntRleDecoder();
                }
                break;
            case Dictionary:
                this.stringEncoder = new DictionaryEncoder();
                this.stringDecoder = new DictionaryDecoder();
                this.isStringEncoded = true;
                break;
            case Snappy:

                break;
            case Zigzag:
                this.timestampEncoder = new LongZigzagEncoder();
                this.timestampDecoder = new LongZigzagDecoder();
                this.longEncoder = new LongZigzagEncoder();
                for (int i = 0; i < longDecoders.length; i++) {
                    longDecoders[i] = new LongZigzagDecoder();
                }
                this.intEncoder = new IntZigzagEncoder();
                for (int i = 0; i < intDecoders.length; i++) {
                    intDecoders[i] = new IntRleDecoder();
                }
                break;
            case Optimize:
                this.timestampEncoder = new LongRleEncoder();
                this.timestampDecoder = new LongRleDecoder();
                this.longEncoder = new LongGorillaEncoder();
                for (int i = 0; i < longDecoders.length; i++) {
                    longDecoders[i] = new LongGorillaDecoder();
                }
                this.intEncoder = new IntRleEncoder();
                for (int i = 0; i < intDecoders.length; i++) {
                    intDecoders[i] = new IntRleDecoder();
                }
                break;
        }
    }

    @Override
    public void storeInput(Object[] myevents, long currentOffset, int interval, String inputStoreCurrentPath) throws IOException, ExecutionException, InterruptedException {
        File file = new File(inputStoreCurrentPath);
        if (!file.exists()) {
            file.mkdirs();
        }
        file = new File(inputStoreCurrentPath + OsUtils.OS_wrapper(taskId + ".input"));
        if (!file.exists())
            file.createNewFile();
        if (!isCompression) {
            storeInputWithoutCompression(myevents, currentOffset, interval, file);
        } else {
            storeInputWithCompression(myevents, currentOffset, interval, file);
        }
    }

    @Override
    public void reloadInput(File inputFile, Queue<Object> lostEvents, long redoOffset, long startOffset, int interval) throws IOException, ExecutionException, InterruptedException {
        if (isCompression) {
            reloadInputWithCompression(inputFile, lostEvents, redoOffset, startOffset, interval);
        } else {
            reloadInputWithoutCompression(inputFile, lostEvents, redoOffset, startOffset, interval);
        }
    }

    private void storeInputWithoutCompression(Object[] myevents, long currentOffset, int interval, File inputFile) throws IOException, ExecutionException, InterruptedException {
        BufferedWriter EventBufferedWriter = new BufferedWriter(new FileWriter(inputFile, true));
        for (int i = (int) currentOffset; i < currentOffset + interval; i++) {
            EventBufferedWriter.write(myevents[i].toString() + "\n");
        }
        EventBufferedWriter.close();
    }

    private void storeInputWithCompression(Object[] myevents, long currentOffset, int interval, File inputFile) throws IOException, ExecutionException, InterruptedException {
//        MeasureTools.BEGIN_COMPRESSION_TIME_MEASURE(this.taskId);
//        Path path = Paths.get(inputFile.getPath());
//        if (!isStringEncoded) {
//            //Type Compression
//            for (int i = (int) currentOffset; i < currentOffset + interval; i++) {
//                if (myevents[i] instanceof TransactionTxnEvent) {
//                    this.intEncoder.encode(0, baos);
//                } else {
//                    this.intEncoder.encode(1, baos);
//                }
//            }
//            this.intEncoder.flush(baos);
//            ByteBuffer byteBuffer0 = ByteBuffer.wrap(baos.toByteArray());
//            int position0 = baos.size();
//            baos.reset();
//            //Timestamp Compression
//            for (int i = (int) currentOffset; i < currentOffset + interval; i++) {
//                this.timestampEncoder.encode(((TxnEvent) myevents[i]).getTimestamp(), baos);
//            }
//            this.timestampEncoder.flush(baos);
//            ByteBuffer byteBuffer1 = ByteBuffer.wrap(baos.toByteArray());
//            int position1 = baos.size() + position0;
//            baos.reset();
//            //Bid Compression
//            for (int i = (int) currentOffset; i < currentOffset + interval; i++) {
//                this.longEncoder.encode(((TxnEvent) myevents[i]).getBid(), baos);
//            }
//            this.longEncoder.flush(baos);
//            ByteBuffer byteBuffer2 = ByteBuffer.wrap(baos.toByteArray());
//            int position2 = baos.size() + position1;
//            baos.reset();
//            //sourceAccountId Compression
//            for (int i = (int) currentOffset; i < currentOffset + interval; i++) {
//                if (myevents[i] instanceof TransactionTxnEvent) {
//                    this.intEncoder.encode(Integer.parseInt(((TransactionTxnEvent) myevents[i]).getSourceAccountId()), baos);
//                } else {
//                    this.intEncoder.encode(Integer.parseInt(((DepositTxnEvent) myevents[i]).getAccountId()), baos);
//                }
//            }
//            this.intEncoder.flush(baos);
//            ByteBuffer byteBuffer3 = ByteBuffer.wrap(baos.toByteArray());
//            int position3 = baos.size() + position2;
//            baos.reset();
//            //sourceAssetId Compression
//            for (int i = (int) currentOffset; i < currentOffset + interval; i++) {
//                if (myevents[i] instanceof TransactionTxnEvent) {
//                    this.intEncoder.encode(Integer.parseInt(((TransactionTxnEvent) myevents[i]).getSourceBookEntryId()), baos);
//                } else {
//                    this.intEncoder.encode(Integer.parseInt(((DepositTxnEvent) myevents[i]).getBookEntryId()), baos);
//                }
//            }
//            this.intEncoder.flush(baos);
//            ByteBuffer byteBuffer4 = ByteBuffer.wrap(baos.toByteArray());
//            int position4 = baos.size() + position3;
//            baos.reset();
//            //accountValue Compression
//            for (int i = (int) currentOffset; i < currentOffset + interval; i++) {
//                if (myevents[i] instanceof TransactionTxnEvent) {
//                    this.longEncoder.encode(((TransactionTxnEvent) myevents[i]).getAccountTransfer(), baos);
//                } else {
//                    this.longEncoder.encode(((DepositTxnEvent) myevents[i]).getAccountTransfer(), baos);
//                }
//            }
//            this.longEncoder.flush(baos);
//            ByteBuffer byteBuffer5 = ByteBuffer.wrap(baos.toByteArray());
//            int position5 = baos.size() + position4;
//            baos.reset();
//            //bookEntryValue Compression
//            for (int i = (int) currentOffset; i < currentOffset + interval; i++) {
//                if (myevents[i] instanceof TransactionTxnEvent) {
//                    this.longEncoder.encode(((TransactionTxnEvent) myevents[i]).getBookEntryTransfer(), baos);
//                } else {
//                    this.longEncoder.encode(((DepositTxnEvent) myevents[i]).getBookEntryTransfer(), baos);
//                }
//            }
//            this.longEncoder.flush(baos);
//            ByteBuffer byteBuffer6 = ByteBuffer.wrap(baos.toByteArray());
//            int position6 = baos.size() + position5;
//            baos.reset();
//            //destinationAccountId Compression only for Transfer event
//            for (int i = (int) currentOffset; i < currentOffset + interval; i++) {
//                if (myevents[i] instanceof TransactionTxnEvent) {
//                    this.intEncoder.encode(Integer.parseInt(((TransactionTxnEvent) myevents[i]).getTargetAccountId()), baos);
//                }
//            }
//            this.intEncoder.flush(baos);
//            ByteBuffer byteBuffer7 = ByteBuffer.wrap(baos.toByteArray());
//            int position7 = baos.size() + position6;
//            baos.reset();
//            //destinationAssetId Compression only for Transfer event
//            for (int i = (int) currentOffset; i < currentOffset + interval; i++) {
//                if (myevents[i] instanceof TransactionTxnEvent) {
//                    this.intEncoder.encode(Integer.parseInt(((TransactionTxnEvent) myevents[i]).getTargetBookEntryId()), baos);
//                }
//            }
//            this.intEncoder.flush(baos);
//            ByteBuffer byteBuffer8 = ByteBuffer.wrap(baos.toByteArray());
//            int position8 = baos.size() + position7;
//            baos.reset();
//            SyncFileAppender appender = new SyncFileAppender(true, path);
//            ByteBuffer metaBuffer = ByteBuffer.allocate(4 * 10);
//            metaBuffer.putInt(0);
//            metaBuffer.putInt((int) (position0 + appender.getPosition().get()));
//            metaBuffer.putInt((int) (position1 + appender.getPosition().get()));
//            metaBuffer.putInt((int) (position2 + appender.getPosition().get()));
//            metaBuffer.putInt((int) (position3 + appender.getPosition().get()));
//            metaBuffer.putInt((int) (position4 + appender.getPosition().get()));
//            metaBuffer.putInt((int) (position5 + appender.getPosition().get()));
//            metaBuffer.putInt((int) (position6 + appender.getPosition().get()));
//            metaBuffer.putInt((int) (position7 + appender.getPosition().get()));
//            metaBuffer.putInt((int) (position8 + appender.getPosition().get()));
//            metaBuffer.flip();
//            MeasureTools.END_COMPRESSION_TIME_MEASURE(this.taskId);
//            MeasureTools.BEGIN_PERSIST_TIME_MEASURE(this.taskId);
//            appender.append(metaBuffer);
//            appender.append(byteBuffer0);
//            appender.append(byteBuffer1);
//            appender.append(byteBuffer2);
//            appender.append(byteBuffer3);
//            appender.append(byteBuffer4);
//            appender.append(byteBuffer5);
//            appender.append(byteBuffer6);
//            appender.append(byteBuffer7);
//            appender.append(byteBuffer8);
//            MeasureTools.END_PERSIST_TIME_MEASURE(this.taskId);
//        } else {
//            MeasureTools.BEGIN_COMPRESSION_TIME_MEASURE(this.taskId);
//            for (int i = (int) currentOffset; i < currentOffset + interval; i++) {
//                stringEncoder.encode(new Binary(myevents[i].toString()), baos);
//            }
//            stringEncoder.flush(baos);
//            ByteBuffer out = ByteBuffer.wrap(baos.toByteArray());
//            baos.reset();
//            MeasureTools.END_COMPRESSION_TIME_MEASURE(this.taskId);
//            MeasureTools.BEGIN_PERSIST_TIME_MEASURE(this.taskId);
//            SyncFileAppender appender = new SyncFileAppender(true, path);
//            appender.append(out);
//            MeasureTools.END_PERSIST_TIME_MEASURE(this.taskId);
//        }
    }

    private void reloadInputWithCompression(File inputFile, Queue<Object> lostEvents, long redoOffset, long startOffset, int interval) throws IOException, ExecutionException, InterruptedException {
//        Path path = Paths.get(inputFile.getPath());
//        AsynchronousFileChannel afc = AsynchronousFileChannel.open(path, READ);
//        int fileSize = (int) afc.size();
//        ByteBuffer dataBuffer = ByteBuffer.allocate(fileSize);
//        Future<Integer> result = afc.read(dataBuffer, 0);
//        result.get();
//        dataBuffer.flip();
//        if (!isStringEncoded) {
//            while (dataBuffer.hasRemaining() && dataBuffer.getInt() == 0) {
//                int position0 = dataBuffer.getInt();
//                int position1 = dataBuffer.getInt();
//                int position2 = dataBuffer.getInt();
//                int position3 = dataBuffer.getInt();
//                int position4 = dataBuffer.getInt();
//                int position5 = dataBuffer.getInt();
//                int position6 = dataBuffer.getInt();
//                int position7 = dataBuffer.getInt();
//                int position8 = dataBuffer.getInt();
//
//                dataBuffer.limit(position0 + 40);
//                ByteBuffer buffer0 = dataBuffer.slice();//Type
//                ByteBuffer buffer1 = getByteBuffer(dataBuffer, position0, position1);//Timestamp
//                ByteBuffer buffer2 = getByteBuffer(dataBuffer, position1, position2);//Bid
//                ByteBuffer buffer3 = getByteBuffer(dataBuffer, position2, position3);//sourceAccountId
//                ByteBuffer buffer4 = getByteBuffer(dataBuffer, position3, position4);//sourceAssetId
//                ByteBuffer buffer5 = getByteBuffer(dataBuffer, position4, position5);//accountValue
//                ByteBuffer buffer6 = getByteBuffer(dataBuffer, position5, position6);//bookEntryValue
//                ByteBuffer buffer7 = getByteBuffer(dataBuffer, position6, position7);//destinationAccountId only for Transfer Event
//                ByteBuffer buffer8 = getByteBuffer(dataBuffer, position7, position8);//destinationAssetId only for Transfer Event
//
//                //Align to offset
//                while (startOffset != redoOffset) {
//                    int type = intDecoders[0].readInt(buffer0);
//                    timestampDecoder.readLong(buffer1);
//                    longDecoders[0].readLong(buffer2);
//                    intDecoders[1].readInt(buffer3);
//                    intDecoders[2].readInt(buffer4);
//                    longDecoders[1].readLong(buffer5);
//                    longDecoders[2].readLong(buffer6);
//                    if (type == 0) {
//                        intDecoders[3].readInt(buffer7);
//                        intDecoders[4].readInt(buffer8);
//                    }
//                    startOffset++;
//                }
//                int number = 0;
//                long groupId = startOffset + interval;
//                while (intDecoders[0].hasNext(buffer0)) {
//                    number++;
//                    if (number == interval) {
//                        groupId += interval;
//                        number = 0;
//                    }
//                    TxnEvent event;
//                    int[] p_bids = new int[tthread];
//                    HashMap<Integer, Integer> pids = new HashMap<>();
//                    int type = intDecoders[0].readInt(buffer0);
//                    long timestamp = timestampDecoder.readLong(buffer1);
//                    long bid = longDecoders[0].readLong(buffer2);
//                    if (historyViews.inspectAbortView(groupId, this.taskId, bid)) {
//                        continue;
//                    }
//                    int sourceAccountId = intDecoders[1].readInt(buffer3);
//                    int npid = (sourceAccountId / partitionOffset);
//                    pids.put((sourceAccountId / partitionOffset), 0);
//                    int sourceAssetId = intDecoders[2].readInt(buffer4);
//                    pids.put((sourceAssetId / partitionOffset), 0);
//                    long accountValue = longDecoders[1].readLong(buffer5);
//                    long bookEntryValue = longDecoders[2].readLong(buffer6);
//                    if (type == 0) {
//                        int destinationAccountId = intDecoders[3].readInt(buffer7);
//                        pids.put((destinationAccountId / partitionOffset), 0);
//                        int destinationAssetId = intDecoders[4].readInt(buffer8);
//                        pids.put((destinationAssetId / partitionOffset), 0);
//                        event = new TransactionTxnEvent(
//                                (int) bid, //bid
//                                npid, //pid
//                                Arrays.toString(p_bids), //bid_arrary
//                                Arrays.toString(pids.keySet().toArray(new Integer[0])), // partition_index
//                                4,//num_of_partition
//                                String.valueOf(sourceAccountId),//getSourceAccountId
//                                String.valueOf(sourceAssetId),//getSourceBookEntryId
//                                String.valueOf(destinationAccountId),//getTargetAccountId
//                                String.valueOf(destinationAssetId),//getTargetBookEntryId
//                                accountValue, //getAccountTransfer
//                                bookEntryValue  //getBookEntryTransfer
//                        );
//                    } else {
//                        event = new DepositTxnEvent(
//                                (int) bid, //bid
//                                npid, //pid
//                                Arrays.toString(p_bids), //bid_array
//                                Arrays.toString(pids.keySet().toArray(new Integer[0])), // partition_index
//                                2,//num_of_partition
//                                String.valueOf(sourceAccountId),//getSourceAccountId
//                                String.valueOf(sourceAssetId),//getSourceBookEntryId
//                                accountValue,  //getAccountDeposit
//                                bookEntryValue  //getBookEntryDeposit
//                        );
//                    }
//                    event.setTimestamp(timestamp);
//                    //TODO:check if the event has been aborted
//                    lostEvents.add(event);
//                }
//            }
//        } else {
//            while (startOffset != redoOffset) {
//                stringDecoder.readBinary(dataBuffer);
//                startOffset++;
//            }
//            int number = 0;
//            long groupId = startOffset + interval;
//            while (stringDecoder.hasNext(dataBuffer)) {
//                String eventString = stringDecoder.readBinary(dataBuffer).getStringValue();
//                TxnEvent event = getEventFromString(eventString, groupId);
//                if (event != null) {
//                    lostEvents.add(event);
//                }
//                number++;
//                if (number == interval) {
//                    groupId += interval;
//                    number = 0;
//                }
//            }
//        }
    }

    private void reloadInputWithoutCompression(File inputFile, Queue<Object> lostEvents, long redoOffset, long startOffset, int interval) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(inputFile)));
        while (startOffset != redoOffset) {
            reader.readLine();
            startOffset++;
        }
        int number = 0;
        long groupId = startOffset + interval;
        String txn = reader.readLine();
        int[] p_bids = new int[tthread];
        while (txn != null) {
            TxnEvent event = getEventFromString(txn, groupId);
            if (event != null) {
                lostEvents.add(event);
            }
            number++;
            if (number == interval) {
                groupId += interval;
                number = 0;
            }
            txn = reader.readLine();
        }
        reader.close();
    }

    private ByteBuffer getByteBuffer(ByteBuffer dataBuffer, int position1, int position2) {
        dataBuffer.position(position1 + 40);
        dataBuffer.limit(position2 + 40);
        return dataBuffer.slice();
    }

    private TxnEvent getEventFromString(String txn, long groupId) {
//        int[] p_bids = new int[tthread];
//        String[] split = txn.split(",");
//        if (FaultToleranceRelax.isAbortPushDown)
//            if (this.ftOption == 3 && historyViews.inspectAbortView(groupId, this.taskId, Integer.parseInt(split[0]))) {
//                return null;
//            }
//        int npid = (int) (Long.parseLong(split[1]) / partitionOffset);
//        HashMap<Integer, Integer> pids = new HashMap<>();
//        if (split.length == 8) {
//            for (int i = 1; i < 5; i++) {
//                pids.put((int) (Long.parseLong(split[i]) / partitionOffset), 0);
//            }
//            TransactionTxnEvent event = new TransactionTxnEvent(
//                    Integer.parseInt(split[0]), //bid
//                    npid, //pid
//                    Arrays.toString(p_bids), //bid_arrary
//                    Arrays.toString(pids.keySet().toArray(new Integer[0])), // partition_index
//                    4,//num_of_partition
//                    split[1],//getSourceAccountId
//                    split[2],//getSourceBookEntryId
//                    split[3],//getTargetAccountId
//                    split[4],//getTargetBookEntryId
//                    Long.parseLong(split[5]), //getAccountTransfer
//                    Long.parseLong(split[6])  //getBookEntryTransfer
//            );
//            event.setTimestamp(Long.parseLong(split[7]));
//            return event;
//        } else {
//            for (int i = 1; i < 3; i++) {
//                pids.put((int) (Long.parseLong(split[i]) / partitionOffset), 0);
//            }
//            DepositTxnEvent event = new DepositTxnEvent(
//                    Integer.parseInt(split[0]), //bid
//                    npid, //pid
//                    Arrays.toString(p_bids), //bid_array
//                    Arrays.toString(pids.keySet().toArray(new Integer[0])), // partition_index
//                    2,//num_of_partition
//                    split[1],//getSourceAccountId
//                    split[2],//getSourceBookEntryId
//                    Long.parseLong(split[3]),  //getAccountDeposit
//                    Long.parseLong(split[4])  //getBookEntryDeposit
//            );
//            event.setTimestamp(Long.parseLong(split[5]));
//            return event;
//        }
        return null;
    }
}
