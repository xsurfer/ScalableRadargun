package org.radargun;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.stages.AbstractBenchmarkStage;
import org.radargun.state.MasterState;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Author: Fabio Perfetti (perfabio87 [at] gmail.com)
 * Date: 8/1/13
 * Time: 2:47 PM
 */
public class ClusterExecutor extends Executor {

    private static Log log = LogFactory.getLog(ClusterExecutor.class);

    private AtomicBoolean running = new AtomicBoolean(false);
    private AtomicBoolean readyToScale = new AtomicBoolean(false);

    protected static final int DEFAULT_READ_BUFF_CAPACITY = 1024;

    private Map<SocketChannel, ByteBuffer> writeBufferMap = new HashMap<SocketChannel, ByteBuffer>();
    private Map<SocketChannel, ByteBuffer> readBufferMap = new HashMap<SocketChannel, ByteBuffer>();

    private List<DistStageAck> responses = new ArrayList<DistStageAck>();

    private Selector communicationSelector;

    private int processedSlaves = 0;

    private CountDownLatch clusterCountDownLatch;

    private MasterState state;

    private List<SocketChannel> slaves = new ArrayList<SocketChannel>();
    private final Map<SocketChannel, Integer> slave2Index = new HashMap<SocketChannel, Integer>();


    public ClusterExecutor(MasterState state){
        this.state = state;
        this.clusterCountDownLatch = new CountDownLatch( state.getConfig().getSlaveCount() );

        try {
            log.info("Opening communicationSelector");
            communicationSelector = Selector.open();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public Boolean call() throws Exception {

        clusterCountDownLatch.await();
        running.compareAndSet(false, true);

        prepareNextStage();
        startCommunicationWithSlaves();

        return null;
    }

    public boolean isRunning(){
        return running.get();
    }

    public boolean isReadyToScale(){
        return readyToScale.get();
    }

    public boolean addSlave(SocketChannel slave){
        if( clusterCountDownLatch.getCount() > 0 ){
            log.debug("Adding a new slave to ClusterExecutor!");
            slaves.add(slave);
            slave2Index.put(slave, slaves.size()-1);
            readBufferMap.put(slave, ByteBuffer.allocate(DEFAULT_READ_BUFF_CAPACITY));

            clusterCountDownLatch.countDown();
            return true;
        }
        return false;
    }



    protected void preSerialization(DistStage toSerialize){
        state.setCurrentMainDistStage(toSerialize);
    }

    @Override
    protected void prePrepareNextStage(){
        // Analyze the acks in order to find some slave stopped
        if ( state.getCurrentDistStage() instanceof AbstractBenchmarkStage ) {
            List<Integer> deadSlaveIndexList = state.sizeForNextStage(localResponses, slaves);
            manageStoppedSlaves(deadSlaveIndexList);

        }
    }


    private void prepareNextStage() throws Exception {
        DistStage toExecute = state.getNextDistStageToProcess();
        if (toExecute == null) {
            //releaseResourcesAndExit();
            throw new RuntimeException("NO OTHER STAGE TO EXECUTE...I NEED TO END");
        } else {
            readyToScale.set(toExecute.isScalable());
            runDistStage(toExecute, toExecute.getActiveSlaveCount());
        }
    }

    private void runDistStage(DistStage currentStage, int noSlaves) throws Exception {
        writeBufferMap.clear();
        DistStage toSerialize;
        for (int i = 0; i < noSlaves; i++) {
            SocketChannel slave = slaves.get(i);
            slave.configureBlocking(false);
            slave.register(communicationSelector, SelectionKey.OP_WRITE);
            toSerialize = currentStage.clone();
            toSerialize.initOnMaster(state, i);
            if (i == 0) {//only log this once
                if (log.isDebugEnabled())
                    log.debug("Starting '" + toSerialize.getClass().getSimpleName() + "' on " + toSerialize.getActiveSlaveCount() + " slave nodes. Details: " + toSerialize);
                else
                    log.info("Starting '" + toSerialize.getClass().getSimpleName() + "' on " + toSerialize.getActiveSlaveCount() + " slave nodes.");
            }
            byte[] bytes = SerializationHelper.prepareForSerialization(toSerialize);
            writeBufferMap.put(slave, ByteBuffer.wrap(bytes));
        }
    }

    private void startCommunicationWithSlaves() throws Exception {
        while (true) {
            communicationSelector.select();
            Set<SelectionKey> keys = communicationSelector.selectedKeys();
            if (keys.size() > 0) {
                Iterator<SelectionKey> keysIt = keys.iterator();
                while (keysIt.hasNext()) {
                    SelectionKey key = keysIt.next();
                    keysIt.remove();
                    if (!key.isValid()) {
                        continue;
                    }
                    if (key.isWritable()) {
                        sendStage(key);
                    } else if (key.isReadable()) {
                        readStageAck(key);
                    } else {
                        log.warn("Unknown selection on key " + key);
                    }
                }
            }
        }
    }

    private void readStageAck(SelectionKey key) throws Exception {
        SocketChannel socketChannel = (SocketChannel) key.channel();

        ByteBuffer byteBuffer = readBufferMap.get(socketChannel);
        int value = socketChannel.read(byteBuffer);

        if (value == -1) {
            log.warn("Slave stopped! Index: " + slave2Index.get(socketChannel) + ". Remote socket is: " + socketChannel);
            key.cancel();
            if (!slaves.remove(socketChannel)) {
                throw new IllegalStateException("Socket " + socketChannel + " should have been there!");
            }
            releaseResourcesAndExit();
        } else if (byteBuffer.limit() >= 4) {
            int expectedSize = byteBuffer.getInt(0);
            if ((expectedSize + 4) > byteBuffer.capacity()) {
                ByteBuffer replacer = ByteBuffer.allocate(expectedSize + 4);
                replacer.put(byteBuffer.array());
                readBufferMap.put(socketChannel, replacer);
                if (log.isTraceEnabled())
                    log.trace("Expected size(" + expectedSize + ")" + " is > ByteBuffer's capacity(" +
                            byteBuffer.capacity() + ")" + ".Replacing " + byteBuffer + " with " + replacer);
                byteBuffer = replacer;
            }
            if (log.isTraceEnabled())
                log.trace("Expected size: " + expectedSize + ". byteBuffer.position() == " + byteBuffer.position());
            if (byteBuffer.position() == expectedSize + 4) {
                log.trace("Received ACK from " + socketChannel);
                DistStageAck ack = (DistStageAck) SerializationHelper.deserialize(byteBuffer.array(), 4, expectedSize);
                byteBuffer.clear();
                responses.add(ack);
            }
        }

        if (responses.size() == state.getSlavesCountForCurrentStage()) {
            if (!state.distStageFinished(responses)) {
                log.error("Exiting because issues processing current stage: " + state.getCurrentDistStage());
                releaseResourcesAndExit();
            }
            prepareNextStage();
        }
    }

    private void sendStage(SelectionKey key) throws IOException {
        SocketChannel socketChannel = (SocketChannel) key.channel();
        ByteBuffer buf = writeBufferMap.get(socketChannel);
        socketChannel.write(buf);
        if (buf.remaining() == 0) {
            log.trace("Finished writing entire buffer");
            key.interestOps(SelectionKey.OP_READ);
            processedSlaves++;
            if (log.isTraceEnabled())
                log.trace("Current stage successfully transmitted to " + processedSlaves + " slave(s).");
        }
        if (processedSlaves == state.getSlavesCountForCurrentStage()) {
            log.trace("Successfully completed broadcasting stage " + state.getCurrentDistStage());
            processedSlaves = 0;
            writeBufferMap.clear();
            responses.clear();
        }
    }

    private void releaseResources() {
        try {
            communicationSelector.close();
        } catch (Throwable e) {
            log.warn(e);
        }
        for (SocketChannel sc : slaves) {
            try {
                sc.socket().close();
            } catch (Throwable e) {
                log.warn(e);
            }
        }

//        try {
//            if (communicationSelector != null) serverSocketChannel.socket().close();
//        } catch (Throwable e) {
//            log.warn(e);
//        }
    }

    private void releaseResourcesAndExit() {
        releaseResources();
        ShutDownHook.exit(0);
    }

}
