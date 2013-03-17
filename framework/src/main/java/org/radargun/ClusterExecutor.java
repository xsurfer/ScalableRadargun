package org.radargun;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: fabio
 * <p/>
 * Posso prendere le info che mi servono dal MasterScaling (singleton) in questo modo:
 * ScalingMaster.getInstance(null);
 * <p/>
 * Date: 11/30/12
 * Time: 3:24 PM
 * To change this template use File | Settings | File Templates.
 */
public class ClusterExecutor implements Runnable {

    private static Log log = LogFactory.getLog(ClusterExecutor.class);

    private List<DistStageAck> responses = new ArrayList<DistStageAck>();

    int processedSlaves = 0;

    private ArrayList<SocketChannel> slaves;

    /**
     * Inizio dei metodi
     */

    public ClusterExecutor(ArrayList<SocketChannel> slaves) {
        try {
            this.slaves = slaves;
            ScalingMaster.getInstance().communicationSelector = Selector.open();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }

        Runtime.getRuntime().addShutdownHook(new ShutDownHook("ClusterExecutor process"));
    }

    @Override
    public void run() {
        try {
            log.debug("Starting...");
            prepareNextStage();
            startCommunicationWithSlaves();
            log.debug("Fine ClusterExecutor");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void prepareNextStage() throws Exception {
        log.debug("Inizio il prepareNextStage");
        DistStage toExecute = ScalingMaster.getInstance().state.getNextDistStageToProcess();
        if (toExecute == null) {
            ShutDownHook.exit(0);
        }
        runDistStage(toExecute, toExecute.getActiveSlaveCount());
    }

    private void runDistStage(DistStage currentStage, int noSlaves) throws Exception {
        log.debug("Inizio il runDistStage");
        ScalingMaster.getInstance().writeBufferMap.clear();

        DistStage toSerialize;
        for (int i = 0; i < noSlaves; i++) {
            SocketChannel slave = ScalingMaster.getInstance().slaves.get(i);
            slave.configureBlocking(false);
            slave.register(ScalingMaster.getInstance().communicationSelector, SelectionKey.OP_WRITE);

            toSerialize = currentStage.clone();
            toSerialize.initOnMaster(ScalingMaster.getInstance().state, i);
            if (i == 0) {//only log this once
                log.info("Starting dist stage '" + toSerialize.getClass().getSimpleName() + "' on " + toSerialize.getActiveSlaveCount() + " Slaves: " + toSerialize);
            }
            byte[] bytes = SerializationHelper.prepareForSerialization(toSerialize);
            ScalingMaster.getInstance().writeBufferMap.put(slave, ByteBuffer.wrap(bytes));
        }
    }

    private void startCommunicationWithSlaves() throws Exception {
        log.debug("Inizio il startCommunicationWithSlaves");
        while (!ScalingMaster.getInstance().stopped) {
            ScalingMaster.getInstance().communicationSelector.select();
            /** Registro nuovi slave nel caso slavesReadyToMerge non è vuota **/
            if(!ScalingMaster.getInstance().slavesReadyToMerge.isEmpty()){
                for (SocketChannel sc : ScalingMaster.getInstance().slavesReadyToMerge) {
                    sc.configureBlocking(false);
                    sc.register(ScalingMaster.getInstance().communicationSelector, SelectionKey.OP_READ);
                }
                ScalingMaster.getInstance().slavesReadyToMerge.clear();
                continue;
            }

            Set<SelectionKey> keys = ScalingMaster.getInstance().communicationSelector.selectedKeys();
            if (log.isTraceEnabled()) log.trace("Received " + keys.size() + " keys.");
            if (keys.size() > 0) {
                Iterator<SelectionKey> keysIt = keys.iterator();
                while (keysIt.hasNext()) {
                    SelectionKey key = keysIt.next();
                    keysIt.remove();
                    //controllo se la chiave appartiene all'insime di slave di questo QuickExecutor
//                    if(this.slaves.contains((SocketChannel) key.channel())){
//                        int slaveNumber = ScalingMaster.getInstance().slave2Index.get((SocketChannel) key.channel());
//                        log.debug("ACK ricevuto da uno Slave" + slaveNumber);
//                        keysIt.remove();
//                    }
//                    else {
//                        continue;
//                    }
                    if (!key.isValid()) {
                        log.trace("Key not valid, skipping!");
                        continue;
                    }
                    if (key.isWritable()) {
                        if (log.isTraceEnabled()) log.trace("Received writable key:" + key);
                        sendStage(key);
                    } else if (key.isReadable()) {
                        if (log.isTraceEnabled()) log.trace("Received readable key:" + key);
                        readStageAck(key);
                    } else {
                        log.warn("Unknown selection on key " + key);
                    }
                }
            }
        }
    }


    /**
     * TODO: riportare le modifiche fatte nell'altro sorgente che permettono di distinguere gli ACK in base allo stage
     */
    private void readStageAck(SelectionKey key) throws Exception {
        SocketChannel socketChannel = (SocketChannel) key.channel();

        ByteBuffer byteBuffer = ScalingMaster.getInstance().readBufferMap.get(socketChannel);
        int value = socketChannel.read(byteBuffer);
        if (log.isTraceEnabled()) {
            log.trace("We've read into the buffer: " + byteBuffer + ". Number of read bytes is " + value);
        }

        if (value == -1) {
            log.warn("Slave stopped! Index: " + ScalingMaster.getInstance().slave2Index.get(socketChannel) + ". Remote socket is: " + socketChannel);
            key.cancel();
            if (!ScalingMaster.getInstance().slaves.remove(socketChannel)) {
                throw new IllegalStateException("Socket " + socketChannel + " should have been there!");
            }
            ScalingMaster.getInstance().releaseResourcesAndExit();
        } else if (byteBuffer.limit() >= 4) {
            int expectedSize = byteBuffer.getInt(0);
            if ((expectedSize + 4) > byteBuffer.capacity()) {
                ByteBuffer replacer = ByteBuffer.allocate(expectedSize + 4);
                replacer.put(byteBuffer.array());
                ScalingMaster.getInstance().readBufferMap.put(socketChannel, replacer);
                if (log.isTraceEnabled())
                    log.trace("Expected size(" + expectedSize + ")" + " is > bytebuffer's capacity(" +
                            byteBuffer.capacity() + ")" + ".Replacing " + byteBuffer + " with " + replacer);
                byteBuffer = replacer;
            }
            if (log.isTraceEnabled())
                log.trace("Expected size: " + expectedSize + ". byteBuffer.position() == " + byteBuffer.position());
            if (byteBuffer.position() == expectedSize + 4) {
                log.trace("Received ACK from " + socketChannel);
                DistStageAck ack = (DistStageAck) SerializationHelper.deserialize(byteBuffer.array(), 4, expectedSize);
                byteBuffer.clear();
                /* controllo se l'ack appartiene allo stage corrente */
                log.info(ack.getStageName());
                log.info(ScalingMaster.getInstance().state.getCurrentDistStage().getClass().getName());
                if (ack.getStageName().compareTo(ScalingMaster.getInstance().state.getCurrentDistStage().getClass().getName()) == 0) {
                    /* TODO: controllare se l'ack proviene da uno slave noto */
                    responses.add(ack);
                    log.debug("ACK added: same stage");
                } else {
                    log.debug("ACK dropped: different stage");
                }
                int ackLeft = ScalingMaster.getInstance(null).state.getSlavesCountForCurrentStage() - responses.size();
                log.info( ackLeft + " ACKs left" );
            }
        }

        if (responses.size() == ScalingMaster.getInstance(null).state.getSlavesCountForCurrentStage()) {
            if (!ScalingMaster.getInstance(null).state.distStageFinished(responses)) {
                log.error("Exiting because issues processing current stage: " + ScalingMaster.getInstance(null).state.getCurrentDistStage());
                ScalingMaster.getInstance(null).releaseResourcesAndExit();
            }

            prepareNextStage();
        }
    }

    private void sendStage(SelectionKey key) throws IOException {
        SocketChannel socketChannel = (SocketChannel) key.channel();
        ByteBuffer buf = ScalingMaster.getInstance(null).writeBufferMap.get(socketChannel);
        if (log.isTraceEnabled())
            log.trace("Writing buffer '" + buf + " to channel '" + socketChannel + "' ");
        socketChannel.write(buf);
        if (log.isTraceEnabled())
            log.trace("Buffer after write: '" + buf + "'");
        if (buf.remaining() == 0) {
            log.trace("Finished writing entire buffer");
            key.interestOps(SelectionKey.OP_READ);
            processedSlaves++;
            if (log.isTraceEnabled())
                log.trace("Current stage successfully transmitted to " + processedSlaves + " slave(s).");
        }
        if (processedSlaves == ScalingMaster.getInstance(null).state.getSlavesCountForCurrentStage()) {
            log.trace("Successfully completed broadcasting stage " + ScalingMaster.getInstance(null).state.getCurrentDistStage());
            processedSlaves = 0;
            ScalingMaster.getInstance(null).writeBufferMap.clear();
            responses.clear();
        }
    }
}
