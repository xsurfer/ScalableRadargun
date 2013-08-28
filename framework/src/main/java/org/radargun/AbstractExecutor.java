package org.radargun;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.state.MasterState;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.Callable;

/**
 * Author: Fabio Perfetti (perfabio87 [at] gmail.com)
 * Date: 8/1/13
 * Time: 5:13 PM
 */
public abstract class AbstractExecutor implements Callable<Boolean> {

    private static Log log = LogFactory.getLog(AbstractExecutor.class);

    protected static final int DEFAULT_READ_BUFF_CAPACITY = 1024;

    protected MasterState state;
    protected final Set<SlaveSocketChannel> slaves;

    protected volatile Map<SocketChannel, ByteBuffer> writeBufferMap = new HashMap<SocketChannel, ByteBuffer>();
    protected volatile Map<SocketChannel, ByteBuffer> readBufferMap = new HashMap<SocketChannel, ByteBuffer>();
    protected List<DistStageAck> responses = new ArrayList<DistStageAck>();
    protected Selector communicationSelector;

    protected int processedSlaves = 0;

    public AbstractExecutor(MasterState state, Set<SlaveSocketChannel> slaves){
        this.slaves = Collections.synchronizedSet(slaves);
        this.state = state;
        try {
            log.info("Opening communicationSelector");
            communicationSelector = Selector.open();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }

    }

    /* TO IMPLEMENT */

    protected abstract boolean assertRunning();

    protected abstract void postStageBroadcast();

    protected abstract int numAckToNextStage();

    protected abstract void slaveStopped(SlaveSocketChannel slave);

    protected abstract void preSerialization(DistStage readyToWriteOnBuffer);

    protected abstract void post();

    /* END*/


    public Boolean call() throws Exception {
        log.trace("Starting a new executor");

        synchronized (slaves){
            for (SlaveSocketChannel ssc : slaves){
                initBuffer(ssc.getSocketChannel());
            }
        }

        prepareNextStage();
        startCommunicationWithSlaves();
        post();
        log.info("End");

        return true;
    }

    protected void initBuffer(SocketChannel socketChannel){
        this.readBufferMap.put(socketChannel, ByteBuffer.allocate(DEFAULT_READ_BUFF_CAPACITY));
    }

    protected void prepareNextStage() throws Exception {
        DistStage toExecute = state.getNextDistStageToProcess();
        if (toExecute == null) {
            releaseResourcesAndExit();
        } else {
            runDistStage(toExecute);
        }
    }

    protected void runDistStage(DistStage currentStage) throws Exception {
        writeBufferMap.clear();
        DistStage toSerialize;
        boolean printFlag = false;
        synchronized (slaves){
            for(SlaveSocketChannel slave : slaves) {
                SocketChannel socketChannel = slave.getSocketChannel();
                socketChannel.configureBlocking(false);
                socketChannel.register(communicationSelector, SelectionKey.OP_WRITE);
                toSerialize = currentStage.clone();
                toSerialize.initOnMaster(state, slave.getId());
                preSerialization(toSerialize);
                if (!printFlag) {//only log this once
                    if (log.isDebugEnabled())
                        log.debug("Starting '" + toSerialize.getClass().getSimpleName() + "' on " + slaves.size() + " slave nodes. Details: " + toSerialize);
                    else
                        log.info("Starting '" + toSerialize.getClass().getSimpleName() + "' on " + slaves.size() + " slave nodes.");
                }
                byte[] bytes = SerializationHelper.prepareForSerialization(toSerialize);
                writeBufferMap.put(socketChannel, ByteBuffer.wrap(bytes));
            }
        }
    }

    protected void startCommunicationWithSlaves() throws Exception {
        while ( assertRunning() ) {
            int numKeys = communicationSelector.select(1000);
            if (numKeys > 0) {
                Set<SelectionKey> keys = communicationSelector.selectedKeys();
                Iterator<SelectionKey> keysIt = keys.iterator();
                while (keysIt.hasNext()) {
                    SelectionKey key = keysIt.next();
                    keysIt.remove();
                    if (!key.isValid()) {
                        continue;
                    }

                    SlaveSocketChannel slave = socketToslave((SocketChannel) key.channel());
                    if( slave == null ){
                        log.info("Slave is null. Skipping");
                        continue;
                    } else if ( !slaves.contains(slave) ){
                        log.info("Doesn't belong to this executor. Skipping");
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
            } else if( numKeys == 0 ) {
                log.trace("Should I merge new nodes?" );
                synchronized (slaves){
                    for(SlaveSocketChannel scc : slaves){
                        scc.getSocketChannel().configureBlocking(false);
                        scc.getSocketChannel().register(communicationSelector, SelectionKey.OP_READ);
                    }
                }
            }
        }
    }

    protected void readStageAck(SelectionKey key) throws Exception {
        SocketChannel socketChannel = (SocketChannel) key.channel();

        ByteBuffer byteBuffer = readBufferMap.get(socketChannel);
        int value = socketChannel.read(byteBuffer);

        if (value == -1) {
            SlaveSocketChannel slave = socketToslave(socketChannel);
            log.warn("Slave stopped! Index: " + slave.getId() + ". Remote socket is: " + socketChannel);
            slaveStopped( slave );
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

                if( ack.isStoppedByJMX() ){
                    slaveStopped( socketToslave(socketChannel) );
                }
            }
        }

        if (responses.size() == numAckToNextStage() ) {
            if (!state.distStageFinished(responses)) {
                log.error("Exiting because issues processing current stage: " + state.getCurrentDistStage());
                releaseResourcesAndExit();
            }
            prepareNextStage();
        } else {
            int ackLeft = numAckToNextStage() - responses.size();
            log.info(ackLeft + " ACKs left for the thread");
        }
    }

    protected void sendStage(SelectionKey key) throws IOException {
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
        if (processedSlaves == slaves.size()) {
            log.trace("Successfully completed broadcasting stage " + state.getCurrentDistStage());
            processedSlaves = 0;
            writeBufferMap.clear();
            responses.clear();
            postStageBroadcast();
        }
    }

    protected void releaseResources() {
        try {
            communicationSelector.close();
        } catch (Throwable e) {
            log.warn(e);
        }
        synchronized (slaves){
            for (SlaveSocketChannel ssc : slaves) {
                try {
                    ssc.getSocketChannel().socket().close();
                } catch (Throwable e) {
                    log.warn(e);
                }
            }
        }
    }

    protected void releaseResourcesAndExit() {
        releaseResources();
        ShutDownHook.exit(0);

    }

    protected SlaveSocketChannel socketToslave(SocketChannel socketChannel){
        synchronized (slaves){
            for (SlaveSocketChannel slave : slaves){
                if(slave.getSocketChannel().equals(socketChannel)){
                    return slave;
                }
            }
        }
        return null;
    }
}
