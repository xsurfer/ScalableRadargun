package org.radargun;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.config.MasterConfig;
import org.radargun.stages.AbstractBenchmarkStage;
import org.radargun.stages.DefaultDistStageAck;
import org.radargun.state.MasterState;
import org.radargun.utils.WorkerThreadFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * This is the master that will coordonate the {@link Slave}s in order to run the benchmark.
 * <p/>
 * Questo è il master, che coordina tutti gli slave. Funziona in questo modo:
 * prepara la socket dopodichè inizia la fase di discovery; una volta raggiunto il numero max di slave per il cluster
 * starta un thread che inizierà a far svolgere tutte attività come di consueto.
 * La fase di discovery non muore ma resta in discovery e quando trova 1 o più slaves che si vogliono aggiungere a runtime
 * starta un altro thread che prima di tutto va ad aggiornare la configurazione principale rappresentata da {@link MasterState}
 * dopodichè inizierà ad eseguiro lo stack di mandatoryStages presenti nel scalingStagesRef che è una variabile locale al thread
 * <p/>
 * L'istanza di questa classe verrà usata in vari thread...ATTENTI ALLA SINCRONIZZAZIONE...per il resto speriamo tutto bene,
 * Buon Natale!
 *
 * @author Mircea.Markus@jboss.com
 */
public class ElasticMaster extends Master {

    private static Log log = LogFactory.getLog(ElasticMaster.class);

    private volatile List<SocketChannel> slavesReadyToMerge = new ArrayList<SocketChannel>();

    private Executor clusterExecutorThread;

    private static final int THREAD_POOL_SIZE = 10;

    public ElasticMaster(MasterConfig masterConfig) {
        super(masterConfig);
    }

    @Override
    protected void setShutDownHook() {
        Runtime.getRuntime().addShutdownHook(new ShutDownHook("Scaling Master process"));
    }

    public void start() throws Exception {
        Thread.currentThread().setName("MainThread");
        log.info("Scaling master starting");
        try {
            startServerSocket();
            runDiscovery();
        } finally {
            releaseResources();
        }
    }

    @Override
    protected void runDiscovery() {
        Discoverer discovererThread = new Discoverer();
        discovererThread.run();
    }

    private void startServerSocket() throws IOException {
        serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.configureBlocking(false);
        InetSocketAddress address;
        if (masterConfig.getHost() == null) {
            address = new InetSocketAddress(masterConfig.getPort());
        } else {
            address = new InetSocketAddress(masterConfig.getHost(), masterConfig.getPort());
        }
        serverSocketChannel.socket().bind(address);
        log.info("Master started and listening for connection on: " + address);
        log.info("Waiting 5 seconds for server socket to open completely");
        try {
            Thread.sleep(5000);
        } catch (InterruptedException ex) {
            // ignore
        }
    }

    private boolean isRunning() {
        return !clusterExecutorThread.stopped;
    }



    /* ******************************************* */
    /* ******** ABSTRACT EXECUTOR CLASS ********** */
    /* ******************************************* */

    abstract class Executor implements Runnable{

        protected Log log;
        protected List<DistStageAck> localResponses = new ArrayList<DistStageAck>();
        protected int processedSlaves = 0;
        protected List<SocketChannel> localSlaves;


        /**
         * This variable is used for the Cluster Executor Thread to check if we have terminated
         * and is used from the Support Executor Threads t
         */
        protected volatile boolean stopped = false;
        private Selector communicationSelector;

        public Executor(List<SocketChannel> slaves) {
            log = LogFactory.getLog(this.getClass());
            this.localSlaves = slaves;

            try {
                communicationSelector = Selector.open();
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public final void run() {
            try {
                init();
                prepareNextStage();
                startCommunicationWithSlaves();
                finalize();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        /**
         * Method to override if you want to execute something before starting the executor
         */
        protected void init(){ log.trace("Init new executor..."); }

        /**
         * Method to override if you want to execute something before ending the executor
         */
        protected void finalize(){ log.trace("Finalize executor"); }

        protected void prepareNextStage() throws Exception {
            DistStage toExecute = state.getNextDistStageToProcess();
            if (toExecute == null) {
                ShutDownHook.exit(0);
            }

            // using getSizeForNextStage() it will send next stage to the right number of slaves (not to the ones stopped by jmx)
            runDistStage(toExecute, ElasticMaster.this.state.getSizeForNextStage());
        }

        protected void runDistStage(DistStage currentStage, int noSlavesToSend) throws Exception {
            ElasticMaster.this.writeBufferMap.clear();

            log.trace("Sending current stage to: " + noSlavesToSend + " slaves");
            DistStage toSerialize;
            for (int i = 0; i < noSlavesToSend; i++) {
                // different way to retrieve the slave. This is most generic avoiding to reimplement it for SupportExecutor
                SocketChannel slave = ElasticMaster.this.slaves.get(ElasticMaster.this.slave2Index.get(localSlaves.get(i)));

                slave.configureBlocking(false);
                slave.register(communicationSelector, SelectionKey.OP_WRITE);
                toSerialize = currentStage.clone();
                toSerialize.initOnMaster(state, ((ElasticMaster.this.slaves.size() - localSlaves.size()) + i));
                preSerialization(toSerialize);
                if (i == 0) {//only log this once
                    log.info("Starting dist stage '" + toSerialize.getClass().getSimpleName() + "' on " + noSlavesToSend + " Slaves: " + toSerialize);
                }
                byte[] bytes = SerializationHelper.prepareForSerialization(toSerialize);
                ElasticMaster.this.writeBufferMap.put(slave, ByteBuffer.wrap(bytes));
            }
        }

        protected void preSerialization(DistStage readyToWriteOnBuffer){}

        private void startCommunicationWithSlaves() throws Exception {
            while (!stopped) {
                communicationSelector.select();
                /** Registro nuovi slave nel caso slavesReadyToMerge non è vuota **/
                if (!slavesReadyToMerge.isEmpty()) {
                    for (SocketChannel sc : slavesReadyToMerge) {
                        sc.configureBlocking(false);
                        sc.register(communicationSelector, SelectionKey.OP_READ);
                    }
                    slavesReadyToMerge.clear();
                    continue;
                }

                Set<SelectionKey> keys = communicationSelector.selectedKeys();
                if (log.isTraceEnabled()) log.trace("Received " + keys.size() + " keys.");
                if (keys.size() > 0) {
                    Iterator<SelectionKey> keysIt = keys.iterator();
                    while (keysIt.hasNext()) {
                        SelectionKey key = keysIt.next();
                        keysIt.remove();



                        if (!key.isValid()) {
                            log.trace("Key not valid, skipping!");
                            continue;
                        }
                        if (key.isWritable()) {
                            if (log.isTraceEnabled()) log.trace("Received writable key:" + key);
                            sendStage(key);
                            if (processedSlaves == ElasticMaster.this.state.getSlavesCountForCurrentStage()) {
                                log.trace("Successfully completed broadcasting stage " + ElasticMaster.this.state.getCurrentDistStage());
                                postStageBroadcast();
                            }
                        } else if (key.isReadable()) {
                            if (log.isTraceEnabled()) log.trace("Received readable key:" + key);
                            readStageAck(key);

                            //TODO: spostare l'informazione localSlaves.size() ossia il numero di slave che stanno processando lo stage
                            //TODO: all'interno di una classe corretta, per ora va bene così
                            if (localResponses.size() == localSlaves.size()) {
                                if (!ElasticMaster.this.state.distStageFinished(localResponses)) {
                                    log.error("Exiting because issues processing current stage: " + ElasticMaster.this.state.getCurrentDistStage());
                                    releaseResourcesAndExit();
                                }
                                prePrepareNextStage();
                                prepareNextStage();
                            }
                        } else {
                            log.warn("Unknown selection on key " + key);
                        }
                    }
                }
            }
        }

        private void readStageAck(SelectionKey key) throws Exception {
            // TODO: riportare le modifiche fatte nell'altro sorgente che permettono di distinguere gli ACK in base allo stage
            SocketChannel socketChannel = (SocketChannel) key.channel();

            ByteBuffer byteBuffer = ElasticMaster.this.readBufferMap.get(socketChannel);
            int value = socketChannel.read(byteBuffer);
            if (log.isTraceEnabled()) {
                log.trace("We've read into the buffer: " + byteBuffer + ". Number of read bytes is " + value);
            }

            if (value == -1) {
                log.warn("Slave stopped! Index: " + slave2Index.get(socketChannel) + ". Remote socket is: " + socketChannel);
                key.cancel();
                manageStoppedSlave(socketChannel);

                /* I've managed slave death so... AVOID TO DIE */
//                if (!ElasticMaster.this.slaves.remove(socketChannel)) {
//                    throw new IllegalStateException("Socket " + socketChannel + " should have been there!");
//                }
//                releaseResourcesAndExit();

            } else if (byteBuffer.limit() >= 4) {
                int expectedSize = byteBuffer.getInt(0);
                if ((expectedSize + 4) > byteBuffer.capacity()) {
                    ByteBuffer replacer = ByteBuffer.allocate(expectedSize + 4);
                    replacer.put(byteBuffer.array());
                    ElasticMaster.this.readBufferMap.put(socketChannel, replacer);
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
                    log.info(ack.getStageName() + " =?= " + ElasticMaster.this.state.getCurrentDistStage().getClass().getName());

                    if (ack.getStageName().compareTo(ElasticMaster.this.state.getCurrentDistStage().getClass().getName()) == 0) {
                        /* TODO: controllare se l'ack proviene da uno slave noto */
                        localResponses.add(ack);
                        log.debug("ACK added: same stage");

                        // Controllo se l'ack che è ricevuto contiene il flag STOPPED a true, nel caso notifico gli slave
                        if(ack instanceof DefaultDistStageAck){
                            DefaultDistStageAck wAck = (DefaultDistStageAck) ack;
                            Object payload = wAck.getPayload();
                            if(payload instanceof Map){
                                Map<String, Object> results = (Map<String, Object>) payload;
                                Object stoppedByJmxObj = results.get("STOPPED");
                                if (stoppedByJmxObj != null){
                                    boolean stoppedByJmx = Boolean.parseBoolean(stoppedByJmxObj.toString());
                                    if(stoppedByJmx){
                                        changeNumSlavesNotify();
                                    }
                                }
                            }
                        }

                    } else {
                        log.debug("ACK dropped: different stage");
                    }
                    int ackLeft = localSlaves.size() - localResponses.size();
                    log.info(ackLeft + " ACKs left for the thread");
                }
            }


        }

        protected void changeNumSlavesNotify(){
            log.info("Notifying all the slaves to change numNodes");

            for(SocketChannel slave : slaves){
                String host = slave.socket().getInetAddress().getHostName();
                String currentBenchmark = ElasticMaster.this.state.getCurrentDistStage().getClass().getName();

                int startIndex = currentBenchmark.lastIndexOf(".")+1;
                int lastIndex =  currentBenchmark.length()-new String("Stage").length();
                currentBenchmark = currentBenchmark.substring(startIndex,lastIndex);

                log.info("CurrentStage: " + currentBenchmark);
                NumNodesJmxRequest jmxRequest = new NumNodesJmxRequest(currentBenchmark, host, NumNodesJmxRequest.DEFAULT_JMX_PORT);
                jmxRequest.doRequest();
            }
        }

        protected void reOrderSlave2Index(){
            slave2Index.clear();
            for(SocketChannel slave : slaves){
                int index = slaves.indexOf(slave);
                slave2Index.put(slave, index);
            }
            log.info("Slave2Index has been reordered");
        }

        public void manageStoppedSlave(SocketChannel socket){
            log.info("Removing slave " + socket.toString() + " because stopped by JMX or unexpectedly died");
            try {
                socket.socket().close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            log.info("Re-mapping hash map");
            reOrderSlave2Index();

            log.debug("New size of slaves: " + ElasticMaster.this.slaves.size());
            log.debug("New size of slave2Index: " + ElasticMaster.this.slave2Index.size());

            if( slaves.size()<1 ){
                log.warn("All slaves dead BEFORE the end of the benchmark");
                releaseResourcesAndExit();
            } else {
                log.info("Updating the currentFixedBenchmark().getSize");
                log.debug("Editing state.getCurrentBenchmark().currentFixedBenchmark().getSize: from " + ElasticMaster.this.state.getCurrentBenchmark().currentFixedBenchmark().getSize() + " to " + ElasticMaster.this.slaves.size());

                ElasticMaster.this.state.getCurrentMainDistStage().setActiveSlavesCount( ElasticMaster.this.slaves.size() );
                ElasticMaster.this.state.getCurrentBenchmark().currentFixedBenchmark().setSize( ElasticMaster.this.slaves.size() );

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

        public void manageStoppedSlaves(List<Integer> deadSlaveIndexList){

            // ordino in ordine discendente in modo che vado a eliminare prima le socket maggiori
            Collections.sort(deadSlaveIndexList, Collections.reverseOrder());

            if (deadSlaveIndexList.size() > 0) {
                for (Integer i : deadSlaveIndexList) {
                    SocketChannel toDelete = ElasticMaster.this.slaves.remove(i.intValue());
                    manageStoppedSlave(toDelete);
                }
            }
        }

        protected void prePrepareNextStage(){}

        private void sendStage(SelectionKey key) throws IOException {
            SocketChannel socketChannel = (SocketChannel) key.channel();
            ByteBuffer buf = ElasticMaster.this.writeBufferMap.get(socketChannel);
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
        }

        protected void postStageBroadcast() {
            processedSlaves = 0;
            ElasticMaster.this.writeBufferMap.clear();
            localResponses.clear();
        }


    }



    /* ******************************************* */
    /* ************* EXECUTOR CLASS ************** */
    /* ******************************************* */

    class ClusterExecutor extends Executor {

        public ClusterExecutor(List<SocketChannel> slaves) { super(slaves); }

        protected void init() {
            super.init();
            Runtime.getRuntime().addShutdownHook(new ShutDownHook("ClusterExecutor process"));
        }

        @Override
        protected void preSerialization(DistStage toSerialize){
            ElasticMaster.this.state.setCurrentMainDistStage(toSerialize);
        }

        @Override
        protected void prePrepareNextStage(){
            // Analyze the acks in order to find some slave stopped
            if (ElasticMaster.this.state.getCurrentDistStage() instanceof AbstractBenchmarkStage) {
                List<Integer> deadSlaveIndexList = ElasticMaster.this.state.sizeForNextStage(localResponses, ElasticMaster.this.slaves);
                manageStoppedSlaves(deadSlaveIndexList);

            }
        }
    }


    /* ******************************************** */
    /* ********** SUPPORT EXECUTOR CLASS ********** */
    /* ******************************************** */

    private class SupportExecutor extends Executor {

        private Selector communicationSelector;

        public SupportExecutor(List<SocketChannel> slaves) {
            super(slaves);
            log = LogFactory.getLog(this.getClass());
        }

        @Override
        protected void init() {
            //Runtime.getRuntime().addShutdownHook(new ShutDownHook(Thread.currentThread().getName() + " process"));

            // int newSize = ElasticMaster.this.masterConfig.getSlaveCount() + localSlaves.size();
            int newSize = ElasticMaster.this.state.getCurrentMainDistStage().getActiveSlaveCount() + localSlaves.size();

            log.info("Updating current benchmark state");

            log.debug("Editing state.getCurrentMainDistStage().getActiveSlaveCount(): from " + ElasticMaster.this.state.getCurrentMainDistStage().getActiveSlaveCount() + " to " + newSize);
            ElasticMaster.this.state.getCurrentMainDistStage().setActiveSlavesCount(newSize);

            log.debug("Editing state.getCurrentBenchmark().currentFixedBenchmark().getSize: from " + ElasticMaster.this.state.getCurrentBenchmark().currentFixedBenchmark().getSize() + " to " + ElasticMaster.this.slaves.size());
            ElasticMaster.this.state.getCurrentBenchmark().currentFixedBenchmark().setSize(ElasticMaster.this.slaves.size());
        }

        @Override
        protected void finalize() {

            for (int i = 0; i < localSlaves.size(); i++) {
                SocketChannel slave = localSlaves.get(i);
                ElasticMaster.this.slavesReadyToMerge.add(slave);
            }
            ElasticMaster.this.clusterExecutorThread.communicationSelector.wakeup();

            changeNumSlavesNotify();

            log.info("Sent data to master, killing!");
        }

        @Override
        protected void prepareNextStage() throws Exception {
            DistStage toExecute = null;

            // salta finchè non è raggiunto il current stage.
            // nel caso in cui lo stage non è skippable ma è settato come RunOnAllSlaves allora lancia una eccezione
            // se lo stage non è skippable e non è RunOnAllSlaves allora devo eseguirlo

            boolean isCurrent = false;
            do {
                toExecute = ElasticMaster.this.state.getNextDistStageToProcess();
                if (toExecute == null) {
                    ShutDownHook.exit(0);
                } else {
                    isCurrent = toExecute.getId().equals(state.getCurrentMainDistStage().getId());
                }

                if (!toExecute.isSkippable() && toExecute.isRunOnAllSlaves()) {
                    throw new IllegalStateException("While adding a slave at runtime, stage " + toExecute.getId() + " cannot be executed on all slaves. If you can't change it, please set it as skippable.");
                }

                if (isCurrent) {
                    log.trace("Reachead Current Main Dist Stage");
                } else if (toExecute.isSkippable()) {
                    log.trace("Skipping the stage [id=" + toExecute.getId() + "; type=" + toExecute.getClass().toString() + "]");
                }


            } while (!isCurrent && toExecute.isSkippable());

            // using localSlaves.size it will send next stage only to new slaves
            runDistStage(toExecute, localSlaves.size());
        }

        @Override
        protected void preSerialization(DistStage readyToWriteOnBuffer){
            if(readyToWriteOnBuffer instanceof AbstractBenchmarkStage){
                ((AbstractBenchmarkStage) readyToWriteOnBuffer).updateTimes((AbstractBenchmarkStage) ElasticMaster.this.state.getCurrentMainDistStage());
            }
        }


        @Override
        protected void postStageBroadcast() {
            super.postStageBroadcast();
            // checking if main current stage has been reached
            if (ElasticMaster.this.state.getCurrentDistStage().getId().equals(ElasticMaster.this.state.getCurrentMainDistStage().getId())) {
                log.info("CurrentMainStage sent to new slave, preparing to quit");
                stopped = true;
            }

        }
    }





    /* ******************************************** */
    /* ************* DISCOVERER CLASS ************* */
    /* ******************************************** */

    private class Discoverer {

        private Log log;
        private Selector discoverySelector;

        private Discoverer() {
            log = LogFactory.getLog(this.getClass());
        }

        public void run() {
            log.info("Executing Discovery Thread");
            try {
                discoverySelector = Selector.open();
                serverSocketChannel.register(discoverySelector, SelectionKey.OP_ACCEPT);
                clusterDiscovery();
                extraSlavesDiscovery();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private void clusterDiscovery() throws IOException {
            /** PRIMA FASE: resto in attesa del numero di slave sufficente per startare il cluster**/
            while (ElasticMaster.this.slaves.size() < masterConfig.getSlaveCount()) {
                log.info("Waiting for " + (masterConfig.getSlaveCount() - ElasticMaster.this.slaves.size()) + " slaves to register to the master.");
                discoverySelector.select();
                Set<SelectionKey> keySet = discoverySelector.selectedKeys();
                Iterator<SelectionKey> it = keySet.iterator();
                while (it.hasNext()) {                                       //Seleziono solamente le chiavi con OP_ACCEPT
                    SelectionKey selectionKey = it.next();
                    it.remove();
                    if (!selectionKey.isValid()) {
                        continue;
                    }
                    ServerSocketChannel srvSocketChannel = (ServerSocketChannel) selectionKey.channel();
                    SocketChannel socketChannel = srvSocketChannel.accept();
                    ElasticMaster.this.slaves.add(socketChannel);
                    slave2Index.put(socketChannel, (ElasticMaster.this.slaves.size() - 1));
                    ElasticMaster.this.readBufferMap.put(socketChannel, ByteBuffer.allocate(DEFAULT_READ_BUFF_CAPACITY));
                    log.info("Discovery: IL readBufferMap ha size: " + readBufferMap.size());
                    if (log.isTraceEnabled())
                        log.trace("Added new slave connection from: " + socketChannel.socket().getInetAddress());
                }
            }
            /** ok adesso siamo pronti per iniziare a formare il cluster **/
            log.info("Connection established from " + ElasticMaster.this.slaves.size() + " slaves.");
            log.info("Ready to start benchmarking on the cluster; Slave number: " + ElasticMaster.this.slaves.size());

            ExecutorService executor = Executors.newSingleThreadExecutor(new WorkerThreadFactory("ClusterExecutor", true));
            ElasticMaster.this.clusterExecutorThread = new ClusterExecutor(ElasticMaster.this.slaves);
            executor.execute(ElasticMaster.this.clusterExecutorThread);
            executor.shutdown();
        }

        private void extraSlavesDiscovery() throws IOException {

            WorkerThreadFactory workerThreadFactory = new WorkerThreadFactory("SupportExecutor", false);
            /* waiting further slaves */
            while (ElasticMaster.this.isRunning()) {
                while (ElasticMaster.this.state.getCurrentMainDistStage() != null && ElasticMaster.this.state.getCurrentMainDistStage().isScalable()) {
                    List<SocketChannel> tmpSlaves = new ArrayList<SocketChannel>(); // variabile che contiene i nuovi slaves da passare al nuovo thread
                    log.info("Waiting for other slaves to register to the master.");
                    discoverySelector.select();
                    Set<SelectionKey> keySet = discoverySelector.selectedKeys();
                    Iterator<SelectionKey> it = keySet.iterator();
                    while (it.hasNext()) {                                       //Seleziono solamente le chiavi con OP_ACCEPT
                        SelectionKey selectionKey = it.next();
                        it.remove();
                        if (!selectionKey.isValid()) {
                            continue;
                        }
                        log.info("*---*---* NEW SLAVE DISCOVERED *---*---*");
                        ServerSocketChannel srvSocketChannel = (ServerSocketChannel) selectionKey.channel();
                        SocketChannel socketChannel = srvSocketChannel.accept();
                        ElasticMaster.this.slaves.add(socketChannel);      /* aggiungo sia alla variabile che contiene tutti gli slave*/
                        tmpSlaves.add(socketChannel);   /* sia alla variabile temporanea che contiene un sott'insieme */
                        slave2Index.put(socketChannel, (ElasticMaster.this.slaves.size() - 1));
                        ElasticMaster.this.readBufferMap.put(socketChannel, ByteBuffer.allocate(DEFAULT_READ_BUFF_CAPACITY));
                        log.trace("Discovery: readBufferMap size: " + ElasticMaster.this.readBufferMap.size());
                        if (log.isTraceEnabled())
                            log.trace("Added new slave connection from: " + socketChannel.socket().getInetAddress());
                    }
                    log.info("Connection established from " + tmpSlaves.size() + " new slaves.");
                    ExecutorService executor = Executors.newSingleThreadExecutor(workerThreadFactory);
                    Runnable worker = new SupportExecutor(new ArrayList<SocketChannel>(tmpSlaves));
                    executor.execute(worker);
                    executor.shutdown();
                }
            }
        }

    }
}
