package org.radargun;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.stages.AbstractBenchmarkStage;
import org.radargun.state.MasterState;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
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

    private Map<SlaveSocketChannel, ByteBuffer> writeBufferMap = new HashMap<SlaveSocketChannel, ByteBuffer>();
    private Map<SlaveSocketChannel, ByteBuffer> readBufferMap = new HashMap<SlaveSocketChannel, ByteBuffer>();

    private List<DistStageAck> responses = new ArrayList<DistStageAck>();

    private Selector communicationSelector;

    private int processedSlaves = 0;

    private CountDownLatch clusterCountDownLatch;

    private MasterState state;

    private List<SlaveSocketChannel> slaves = new ArrayList<SlaveSocketChannel>();


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

    public boolean addSlave(SlaveSocketChannel slave){
        if( clusterCountDownLatch.getCount() > 0 ){
            log.debug("Adding a new slave to ClusterExecutor!");
            slaves.add(slave);
            readBufferMap.put(slave, ByteBuffer.allocate(DEFAULT_READ_BUFF_CAPACITY));

            clusterCountDownLatch.countDown();
            return true;
        }
        return false;
    }

    public void mergeSlave(List<SocketChannel> slaves){
        for (SocketChannel sc : slaves) {
            try {
                sc.configureBlocking(false);
                sc.register(communicationSelector, SelectionKey.OP_READ);
            } catch (IOException e) {
                log.info(e, e);
                throw new RuntimeException(e);
            }
        }
        communicationSelector.wakeup();
    }



    private void preSerialization(DistStage toSerialize){
        state.setCurrentMainDistStage(toSerialize);
    }


    private void manageStoppedSlave(SlaveSocketChannel slaveSocketChannel){
        log.info("Removing slave " + slaveSocketChannel.getId() + " because stopped by JMX or unexpectedly died");
        try {
            slaveSocketChannel.getSocketChannel().socket().close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        slaves.remove(slaveSocketChannel);


        log.debug("New size of slaves: " + slaves.size());

        if( slaves.size()<1 ){
            log.warn("All slaves dead BEFORE the end of the benchmark");
            releaseResourcesAndExit();
        } else {
            log.info("Updating the currentFixedBenchmark().getSize");
            log.debug("Editing state.getCurrentBenchmark().currentFixedBenchmark().getSize: from " + state.getCurrentBenchmark().currentFixedBenchmark().getSize() + " to " + slaves.size());

            state.getCurrentMainDistStage().setActiveSlavesCount(slaves.size());
            state.getCurrentBenchmark().currentFixedBenchmark().setSize(slaves.size());

                    /*
                    int newSize = ElasticMaster.this.state.getCurrentMainDistStage().getActiveSlaveCount() -1;
                    log.debug("Decremento state.getCurrentMainDistStage().setActiveSlavesCount da: " +
                            ElasticMaster.this.state.getCurrentMainDistStage().getActiveSlaveCount() +
                            " a " +
                            newSize);
                    //ElasticMaster.this.state.getCurrentMainDistStage().setActiveSlavesCount(newSize);

                    log.debug("Editing state.getCurrentBenchmark().currentFixedBenchmark().getSize: from " +
                            ElasticMaster.this.state.getCurrentBenchmark().currentFixedBenchmark().getSize() +
                            " to " + ElasticMaster.this.slaves.size());
                    ElasticMaster.this.state.getCurrentBenchmark().currentFixedBenchmark().setSize(ElasticMaster.this.slaves.size());
                    */
        }
    }

    private void manageStoppedSlaves(List<Integer> deadSlaveIndexList){

        // ordino in ordine discendente in modo che vado a eliminare prima le socket maggiori
        Collections.sort(deadSlaveIndexList, Collections.reverseOrder());

        if (deadSlaveIndexList.size() > 0) {
            for (Integer i : deadSlaveIndexList) {
                SocketChannel toDelete = slaves.remove(i);
                manageStoppedSlave(toDelete);
            }
        }
    }

    protected void prePrepareNextStage(){
        // Analyze the acks in order to find some slave stopped
        if ( state.getCurrentDistStage() instanceof AbstractBenchmarkStage ) {
            List<Integer> deadSlaveIndexList = state.sizeForNextStage(responses, slaves);
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
            SlaveSocketChannel slave = slaves.get(i);
            slave.getSocketChannel().configureBlocking(false);
            slave.getSocketChannel().register(communicationSelector, SelectionKey.OP_WRITE);
            toSerialize = currentStage.clone();
            toSerialize.initOnMaster(state, i);
            preSerialization(toSerialize);
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

    private SlaveSocketChannel socketChannel2slaveSocketChannel(SocketChannel socketChannel){
        for (SlaveSocketChannel slaveSocketChannel : slaves){
            if( socketChannel.equals(slaveSocketChannel.getSocketChannel()) ) {
                return slaveSocketChannel;
            }
        }
        return null;
    }

    private void readStageAck(SelectionKey key) throws Exception {
        SocketChannel socketChannel = (SocketChannel) key.channel();

        ByteBuffer byteBuffer = readBufferMap.get(socketChannel);
        int value = socketChannel.read(byteBuffer);

        if (value == -1) {
            SlaveSocketChannel slaveSocketChannel = socketChannel2slaveSocketChannel(socketChannel);
            if(slaveSocketChannel==null)
                throw new RuntimeException("SlaveSocketChannel not found!!!");

            log.warn("Slave stopped! Index: " + slaveSocketChannel.getId() + ". Remote socket is: " + socketChannel);
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
            prePrepareNextStage();
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
