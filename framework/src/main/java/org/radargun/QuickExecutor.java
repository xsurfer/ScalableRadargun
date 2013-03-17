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
 * Questa classe è quella che starta rapidamente un insieme di slaves. Cosa fà?
 * <p/>
 * - Aggiorna il numero di slaves presenti nel MasterConfig/MasterState
 * - Inizia a far eseguire lo stack di stage dedicati agli hot slave
 * <p/>
 * User: fabio
 * Date: 11/30/12
 * Time: 7:15 PM
 */
public class QuickExecutor extends Observable implements Runnable {

    private static Log log = LogFactory.getLog(QuickExecutor.class);

    private Selector communicationSelector;

    private List<DistStageAck> responses = new ArrayList<DistStageAck>();

    private boolean quickThreadRunning = true;
//    // questa variabile contiene informazioni (running) sulle istanze dei thread responsabili di far fare il join di slaves
//    public ThreadLocal<Boolean> quickThreadRunning = new ThreadLocal<Boolean>(){
//        protected Boolean initialValue() {
//            return new Boolean(true);
//        }
//    };

    int processedSlaves = 0;

    private final List<SocketChannel> newSlaves;

    public QuickExecutor(List<SocketChannel> newSlaves) {
        this.newSlaves = newSlaves;
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
        //newSlaves.set(tmpSlaves);
        try {
            /*
            * 16Mar2013 - perchè aggiornavo il numero di slave nel master conf
            * oggi direi che non è necessario in quanto se voglio mantenere la feature dello scalingtest (quello originale di radargun)
            * non devo cambiare il conf in modo che quando rinizia ricomincia col numero giusto di slave,
            * nel caso poi si devono riaggiungere i nodi!
            */
            log.warn("Commentato le modifiche sul MasterConfig: inutile altrimenti dovrei fare un reset");
            int newSize = ScalingMaster.getInstance().masterConfig.getSlaveCount() + newSlaves.size();
            //log.debug("Editing slaves number on masterConfig: from " + ScalingMaster.getInstance().masterConfig.getSlaveCount() + " to " + newSize);
            //ScalingMaster.getInstance().masterConfig.setSlavesCount(newSize);

            log.debug("Editing slaves number on the current stage: from " + ScalingMaster.getInstance().state.getCurrentDistStage() + " to " + newSize);
            ScalingMaster.getInstance().state.getCurrentDistStage().setActiveSlavesCount(newSize);

            log.debug("Editing size for the current fixed benchmark");
            ScalingMaster.getInstance().state.getCurrentBenchmark().currentFixedBenchmark().setSize(newSize);

            prepareNextStage();
            startCommunicationWithSlaves();

            log.debug("Ended!");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void prepareNextStage() throws Exception {
        log.debug("Executing prepareNextStage()");
        DistStage toExecute = ScalingMaster.getInstance().state.getNextDistScalingStageToProcess();
        if (toExecute == null) {
            ShutDownHook.exit(0);
            //return;
        }
        log.warn("PLACEHOLDER: QuickExecutor->prepareNextStage ["+newSlaves.size()+"!="+toExecute.getActiveSlaveCount()+"]");
        // Eseguo lo stage con active slave pari a clusterSize+newSlaves ma poi invio/ricevo stage/ack dagli slavi attuali
        runDistStage(toExecute, toExecute.getActiveSlaveCount());
    }

    private void runDistStage(DistStage currentStage, int noSlaves) throws Exception {
        log.debug("Executing runDistStage; noSlaves=" + noSlaves + "=?=" + newSlaves.size() + "=newSlaves.size");
        ScalingMaster.getInstance().writeBufferMap.clear();

        DistStage toSerialize;
        for (int i = 0; i < newSlaves.size(); i++) {
            /** Qui sotto, a differenza di prima, devo eseguire gli stage solamente in quegli slave nuovi quindi per fare
             *  questo prendo l'indice dello slave tramite lo slave2Index dello Scaling Master. Dopodichè uso quest'indice
             *  per recuperare la socket
             *  SocketChannel slave = ScalingMaster.getInstance().slaves.get( ScalingMaster.getInstance().slave2Index.get(newSlaves.get().get(i)) );
             *
             *  altriemnti più semplicemente scorro gli slave passati
             **/
            SocketChannel slave = newSlaves.get(i);
            slave.configureBlocking(false);
            slave.register(communicationSelector, SelectionKey.OP_WRITE);

            toSerialize = currentStage.clone();
            toSerialize.initOnMaster(ScalingMaster.getInstance().state, ScalingMaster.getInstance().slave2Index.get(slave));
            if (i == 0) {//only log this once
                log.warn("PLACEHORLDER: QuickExecutor->runDistStage");
                log.info("Starting dist stage '" + toSerialize.getClass().getSimpleName() + "' on " + this.newSlaves.size() + " Slaves: " + toSerialize);
                //log.info("Starting dist stage '" + toSerialize.getClass().getSimpleName() + "' on " + toSerialize.getActiveScalingSlavesCount() + " Slaves: " + toSerialize);
            }
            byte[] bytes = SerializationHelper.prepareForSerialization(toSerialize);
            ScalingMaster.getInstance().writeBufferMap.put(slave, ByteBuffer.wrap(bytes));
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
                    if (this.newSlaves.contains((SocketChannel) key.channel())) {
                        int slaveNumber = ScalingMaster.getInstance().slave2Index.get((SocketChannel) key.channel());
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
            if (!quickThreadRunning) {
                log.info("Ok, abbiamo appena inviato lo state del Benchmark, possiamo morire!");
                for (int i = 0; i < newSlaves.size(); i++) {
                    SocketChannel slave = newSlaves.get(i);
                    ScalingMaster.getInstance().slavesReadyToMerge.add(slave);
                }
                ScalingMaster.getInstance().communicationSelector.wakeup();
                break;
            }
        }
    }


    /**
     *
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
                log.debug(ack.getStageName());
                log.debug(ScalingMaster.getInstance().state.getCurrentDistScalingStage().getClass().getName());
                if (ack.getStageName().compareTo(ScalingMaster.getInstance().state.getCurrentDistScalingStage().getClass().getName()) == 0) {
                    responses.add(ack);
                    log.info("ACK added: same stage");
                } else {
                    log.info("ACK dropped: different stage");
                }
            }
        }

        if (responses.size() == this.newSlaves.size()) {
            if (!ScalingMaster.getInstance().state.distScalingStageFinished(responses)) {
                log.error("Exiting because issues processing current stage: " + ScalingMaster.getInstance().state.getCurrentDistStage());
                ScalingMaster.getInstance().releaseResourcesAndExit();
            }

            prepareNextStage();
        } else {
            log.warn("PLACEHOLDER: QuickExecutor->ReadStageAck");
            //int ackLeft = ScalingMaster.getInstance().state.getSlavesCountForCurrentScalingStage() - responses.size();
            int ackLeft = this.newSlaves.size() - responses.size();
            log.info(ackLeft + "ACKs left for this scaling thread [" +Thread.currentThread().toString()+ "]");
        }
    }


    private void sendStage(SelectionKey key) throws IOException {
        SocketChannel socketChannel = (SocketChannel) key.channel();
        ByteBuffer buf = ScalingMaster.getInstance().writeBufferMap.get(socketChannel);
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

            //log.info(ScalingMaster.getInstance().state.getCurrentDistScalingStage().getClass().getName());
            // org.radargun.mandatoryStages.WebSessionBenchmarkStage

//            if(ScalingMaster.getInstance().state.getCurrentDistScalingStage().getClass().getName().contains("TpccBenchmarkStage")){

            if(ScalingMaster.getInstance().state.getCurrentDistStage().getId().equals(ScalingMaster.getInstance().state.getCurrentDistScalingStage().getId())){
                log.debug("Ho appena inviato lo stage per il benchmark, setto a false la variabile di questo thread per terminare");
                quickThreadRunning = false;
            }
        }
        log.warn("PLACEHOLDER: QuickExecutor->sendStage");
        //if (processedSlaves == ScalingMaster.getInstance().state.getSlavesCountForCurrentScalingStage()) {
        if (processedSlaves == this.newSlaves.size()) {
            log.trace("Successfully completed broadcasting stage " + ScalingMaster.getInstance().state.getCurrentDistScalingStage());
            processedSlaves = 0;
            ScalingMaster.getInstance().writeBufferMap.clear();
            responses.clear();
        }
    }
}
