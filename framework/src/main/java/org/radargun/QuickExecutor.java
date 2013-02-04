package org.radargun;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Questa classe è quella che starta rapidamente un insieme di slaves. Cosa fà?
 * <p/>
 * - Aggiorna il numero di slaves presenti nel MasterConfig/MasterState
 * - Inizia a far eseguire lo stack di stage dedicati agli hot slave
 * <p/>
 * User: fabio
 * Date: 11/30/12
 * Time: 7:15 PM
 */
public class QuickExecutor implements Runnable {

    private static Log log = LogFactory.getLog(QuickExecutor.class);

    private Selector communicationSelector;

    /**
     * TODO: Questa variabile deve essere locale al thread *
     */
    private List<DistStageAck> responses = new ArrayList<DistStageAck>();

    int processedSlaves = 0;

    List<SocketChannel> tmpSlaves;

    protected ThreadLocal<List<SocketChannel>> newSlavesRef = new ThreadLocal<List<SocketChannel>>() {
        protected List<SocketChannel> initialValue() {
            return new ArrayList<SocketChannel>(tmpSlaves);
        }
    };

    /**
     * Inizio dei metodi
     */

    public QuickExecutor(List<SocketChannel> newSlaves) {
        this.tmpSlaves = newSlaves;
        try {
            communicationSelector = Selector.open();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }

        Runtime.getRuntime().addShutdownHook(new ShutDownHook("QuickExecutor process"));
    }

    @Override
    public void run() {
        log.debug("Starting...");
        newSlavesRef.set(tmpSlaves);
        try {
            // aggiorno il numero di slaves nel master config
            int newSize = ScalingMaster.getMaster(null).masterConfig.getSlaveCount() + newSlavesRef.get().size();
            log.debug("Editing slaves number on masterConfig: from " + ScalingMaster.getMaster(null).masterConfig.getSlaveCount() + " to " + newSize);
            ScalingMaster.getMaster(null).masterConfig.setSlavesCount(newSize);
            ScalingMaster.getMaster(null).state.getCurrentDistStage().setActiveSlavesCount(newSize);

            prepareNextStage();
            startCommunicationWithSlaves();

            log.debug("Ended!");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void prepareNextStage() throws Exception {
        log.debug("Executing prepareNextStage()");
        DistStage toExecute = ScalingMaster.getMaster(null).state.getNextDistScalingStageToProcess(newSlavesRef.get().size());
        if (toExecute == null) {
            ShutDownHook.exit(0);
        }
        runDistStage(toExecute, toExecute.getActiveScalingSlavesCount());
    }

    private void runDistStage(DistStage currentStage, int noSlaves) throws Exception {
        log.debug("Executing runDistStage; noSlaves=" + noSlaves + "=?=" + newSlavesRef.get().size() + "=newSlavesRef.size");
        ScalingMaster.getMaster(null).writeBufferMap.clear();

        DistStage toSerialize;
        for (int i = 0; i < newSlavesRef.get().size(); i++) {
            /** Qui sotto, a differenza di prima, devo eseguire gli stage solamente in quegli slave nuovi quindi per fare
             *  questo prendo l'indice dello slave tramite lo slave2Index dello Scaling Master. Dopodichè uso quest'indice
             *  per recuperare la socket
             *  SocketChannel slave = ScalingMaster.getMaster(null).slaves.get( ScalingMaster.getMaster(null).slave2Index.get(newSlavesRef.get().get(i)) );
             *
             *  altriemnti più semplicemente scorro gli slave passati
             **/
            SocketChannel slave = newSlavesRef.get().get(i);
            slave.configureBlocking(false);
            slave.register(communicationSelector, SelectionKey.OP_WRITE);

            toSerialize = currentStage.clone();
            toSerialize.initOnMaster(ScalingMaster.getMaster(null).state, ScalingMaster.getMaster(null).slave2Index.get(slave));
            if (i == 0) {//only log this once
                log.info("Starting dist stage '" + toSerialize.getClass().getSimpleName() + "' on " + toSerialize.getActiveScalingSlavesCount() + " Slaves: " + toSerialize);
            }
            byte[] bytes = SerializationHelper.prepareForSerialization(toSerialize);
            ScalingMaster.getMaster(null).writeBufferMap.put(slave, ByteBuffer.wrap(bytes));
        }
    }

    private void startCommunicationWithSlaves() throws Exception {
        /** Questo ciclo deve eseguire finchè non arrivo alla fase di benchmark dopodichè muore il thread **/
        while (true) {
            communicationSelector.select();
            Set<SelectionKey> keys = communicationSelector.selectedKeys();
            if (log.isTraceEnabled()) log.trace("Received " + keys.size() + " keys.");
            if (keys.size() > 0) {
                Iterator<SelectionKey> keysIt = keys.iterator();
                while (keysIt.hasNext()) {
                    SelectionKey key = keysIt.next();

                    //controllo se la chiave appartiene all'insime di slave di questo QuickExecutor
                    if (this.newSlavesRef.get().contains((SocketChannel) key.channel())) {
                        int slaveNumber = ScalingMaster.getMaster(null).slave2Index.get((SocketChannel) key.channel());
                        log.debug("ACK ricevuto da uno Slave" + slaveNumber);
                        keysIt.remove();
                    } else {
                        continue;
                    }

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
            if (!ScalingMaster.getMaster(null).quickThreadRunning.get()) {
                log.info("Ok, abbiamo appena inviato lo state del Benchmark, possiamo morire!");
                for (int i = 0; i < newSlavesRef.get().size(); i++) {
                    SocketChannel slave = newSlavesRef.get().get(i);
                    ScalingMaster.getMaster(null).slavesReadyToMerge.add(slave);
                }
                ScalingMaster.getMaster(null).communicationSelector.wakeup();
                break;
            }
        }
    }


    /**
     *
     */
    private void readStageAck(SelectionKey key) throws Exception {
        SocketChannel socketChannel = (SocketChannel) key.channel();

        ByteBuffer byteBuffer = ScalingMaster.getMaster(null).readBufferMap.get(socketChannel);
        int value = socketChannel.read(byteBuffer);
        if (log.isTraceEnabled()) {
            log.trace("We've read into the buffer: " + byteBuffer + ". Number of read bytes is " + value);
        }

        if (value == -1) {
            log.warn("Slave stopped! Index: " + ScalingMaster.getMaster(null).slave2Index.get(socketChannel) + ". Remote socket is: " + socketChannel);
            key.cancel();
            if (!ScalingMaster.getMaster(null).slaves.remove(socketChannel)) {
                throw new IllegalStateException("Socket " + socketChannel + " should have been there!");
            }
            ScalingMaster.getMaster(null).releaseResourcesAndExit();
        } else if (byteBuffer.limit() >= 4) {
            int expectedSize = byteBuffer.getInt(0);
            if ((expectedSize + 4) > byteBuffer.capacity()) {
                ByteBuffer replacer = ByteBuffer.allocate(expectedSize + 4);
                replacer.put(byteBuffer.array());
                ScalingMaster.getMaster(null).readBufferMap.put(socketChannel, replacer);
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
                log.debug(ack.getStageName());
                log.debug(ScalingMaster.getMaster(null).state.getCurrentDistScalingStage().getClass().getName());
                if (ack.getStageName().compareTo(ScalingMaster.getMaster(null).state.getCurrentDistScalingStage().getClass().getName()) == 0) {
                    responses.add(ack);
                    log.info("ACK added: same stage");
                } else {
                    log.info("ACK dropped: different stage");
                }
            }
        }

        if (responses.size() == this.newSlavesRef.get().size()) {
            if (!ScalingMaster.getMaster(null).state.distScalingStageFinished(responses)) {
                log.error("Exiting because issues processing current stage: " + ScalingMaster.getMaster(null).state.getCurrentDistStage());
                ScalingMaster.getMaster(null).releaseResourcesAndExit();
            }

            prepareNextStage();
        } else {
            int ackLeft = ScalingMaster.getMaster(null).state.getSlavesCountForCurrentScalingStage() - responses.size();
            log.info("Mancano " + ackLeft + "ACKs per questo Scaling Thread: " + Thread.currentThread().toString());
        }
    }


    private void sendStage(SelectionKey key) throws IOException {
        SocketChannel socketChannel = (SocketChannel) key.channel();
        ByteBuffer buf = ScalingMaster.getMaster(null).writeBufferMap.get(socketChannel);
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

            //log.info(ScalingMaster.getMaster(null).state.getCurrentDistScalingStage().getClass().getName());
            // org.radargun.stages.WebSessionBenchmarkStage
            if(ScalingMaster.getMaster(null).state.getCurrentDistScalingStage().getClass().getName().contains("TpccBenchmarkStage")){
                log.debug("Ho appena inviato lo stage per il benchmark, setto a false la variabile di questo thread per terminare");
                ScalingMaster.getMaster(null).quickThreadRunning.set(new Boolean(false));
            }
        }
        if (processedSlaves == ScalingMaster.getMaster(null).state.getSlavesCountForCurrentScalingStage()) {
            log.trace("Successfully completed broadcasting stage " + ScalingMaster.getMaster(null).state.getCurrentDistScalingStage());
            processedSlaves = 0;
            ScalingMaster.getMaster(null).writeBufferMap.clear();
            responses.clear();
        }
    }
}
