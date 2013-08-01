//package org.radargun;
//
//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
//import org.radargun.stages.AbstractBenchmarkStage;
//import org.radargun.utils.WorkerThreadFactory;
//
//import java.io.IOException;
//import java.nio.ByteBuffer;
//import java.nio.channels.SelectionKey;
//import java.nio.channels.Selector;
//import java.nio.channels.ServerSocketChannel;
//import java.nio.channels.SocketChannel;
//import java.util.ArrayList;
//import java.util.Iterator;
//import java.util.List;
//import java.util.Set;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.Executors;
//
///**
// * Author: Fabio Perfetti (perfabio87 [at] gmail.com)
// * Date: 8/1/13
// * Time: 6:44 PM
// */
//public class SupportExecutor {
//
//    private static Log log = LogFactory.getLog(SupportExecutor.class);
//
//    private Selector communicationSelector;
//
//    public SupportExecutor(List<SocketChannel> slaves) {
//        super(slaves);
//        log = LogFactory.getLog(this.getClass());
//    }
//
//    @Override
//    protected void init() {
//        log.info( "??????????????????????? SUPPORT EXECUTOR INIT ???????????????????????");
//        //Runtime.getRuntime().addShutdownHook(new ShutDownHook(Thread.currentThread().getName() + " process"));
//
//        // int newSize = ElasticMaster.this.masterConfig.getSlaveCount() + localSlaves.size();
//
//    }
//
//    @Override
//    protected void finalize() {
//
//        for (int i = 0; i < localSlaves.size(); i++) {
//            SocketChannel slave = localSlaves.get(i);
//            ElasticMaster.this.slavesReadyToMerge.add(slave);
//        }
//        ElasticMaster.this.clusterExecutorThread.communicationSelector.wakeup();
//
//        changeNumSlavesNotify();
//
//        log.info("Sent data to master, killing!");
//    }
//
//    @Override
//    protected void prepareNextStage() throws Exception {
//        DistStage toExecute = null;
//
//        // salta finchè non è raggiunto il current stage.
//        // nel caso in cui lo stage non è skippable ma è settato come RunOnAllSlaves allora lancia una eccezione
//        // se lo stage non è skippable e non è RunOnAllSlaves allora devo eseguirlo
//
//        boolean isCurrent = false;
//        do {
//            toExecute = ElasticMaster.this.state.getNextDistStageToProcess();
//            if (toExecute == null) {
//                ShutDownHook.exit(0);
//            } else {
//                isCurrent = toExecute.getId().equals(state.getCurrentMainDistStage().getId());
//            }
//
//            if (!toExecute.isSkippable() && toExecute.isRunOnAllSlaves()) {
//                throw new IllegalStateException("While adding a slave at runtime, stage " + toExecute.getId() + " cannot be executed on all slaves. If you can't change it, please set it as skippable.");
//            }
//
//            if (isCurrent) {
//                log.trace("Reachead Current Main Dist Stage");
//            } else if (toExecute.isSkippable()) {
//                log.trace("Skipping the stage [id=" + toExecute.getId() + "; type=" + toExecute.getClass().toString() + "]");
//            }
//
//
//        } while (!isCurrent && toExecute.isSkippable());
//
//        // using localSlaves.size it will send next stage only to new slaves
//        runDistStage(toExecute, localSlaves.size());
//    }
//
//    @Override
//    protected void preSerialization(DistStage readyToWriteOnBuffer){
//        if(readyToWriteOnBuffer instanceof AbstractBenchmarkStage){
//            ((AbstractBenchmarkStage) readyToWriteOnBuffer).updateTimes((AbstractBenchmarkStage) ElasticMaster.this.state.getCurrentMainDistStage());
//        }
//    }
//
//
//    @Override
//    protected void postStageBroadcast() {
//        super.postStageBroadcast();
//        // checking if main current stage has been reached
//        if (ElasticMaster.this.state.getCurrentDistStage().getId().equals(ElasticMaster.this.state.getCurrentMainDistStage().getId())) {
//            log.info("CurrentMainStage sent to new slave, preparing to quit");
//            stopped = true;
//        }
//
//    }
//}
//
//
//
//
//
//    /* ******************************************** */
//    /* ************* DISCOVERER CLASS ************* */
//    /* ******************************************** */
//
//private class Discoverer {
//
//    private Log log = LogFactory.getLog(this.getClass());
//    private Selector discoverySelector;
//
//    private Discoverer() {
//
//    }
//
//    public void run() {
//        log.info("Executing Discovery Thread");
//        try {
//            discoverySelector = Selector.open();
//            serverSocketChannel.register(discoverySelector, SelectionKey.OP_ACCEPT);
//            clusterDiscovery();
//            extraSlavesDiscovery();
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
//    }
//
//    private void clusterDiscovery() throws IOException {
//        /** PRIMA FASE: resto in attesa del numero di slave sufficente per startare il cluster**/
//        while (ElasticMaster.this.slaves.size() < masterConfig.getSlaveCount()) {
//            log.info("Waiting for " + (masterConfig.getSlaveCount() - ElasticMaster.this.slaves.size()) + " slaves to register to the master.");
//            discoverySelector.select();
//            Set<SelectionKey> keySet = discoverySelector.selectedKeys();
//            Iterator<SelectionKey> it = keySet.iterator();
//            while (it.hasNext()) {                                       //Seleziono solamente le chiavi con OP_ACCEPT
//                SelectionKey selectionKey = it.next();
//                it.remove();
//                if (!selectionKey.isValid()) {
//                    continue;
//                }
//                ServerSocketChannel srvSocketChannel = (ServerSocketChannel) selectionKey.channel();
//                SocketChannel socketChannel = srvSocketChannel.accept();
//                ElasticMaster.this.slaves.add(socketChannel);
//                slave2Index.put(socketChannel, (ElasticMaster.this.slaves.size() - 1));
//                ElasticMaster.this.readBufferMap.put(socketChannel, ByteBuffer.allocate(DEFAULT_READ_BUFF_CAPACITY));
//                log.info("Discovery: IL readBufferMap ha size: " + readBufferMap.size());
//                if (log.isTraceEnabled())
//                    log.trace("Added new slave connection from: " + socketChannel.socket().getInetAddress());
//            }
//        }
//        /** ok adesso siamo pronti per iniziare a formare il cluster **/
//        log.info("Connection established from " + ElasticMaster.this.slaves.size() + " slaves.");
//        log.info("Ready to start benchmarking on the cluster; Slave number: " + ElasticMaster.this.slaves.size());
//
//        ExecutorService executor = Executors.newSingleThreadExecutor(new WorkerThreadFactory("ClusterExecutor", true));
//        ElasticMaster.this.clusterExecutorThread = new ClusterExecutor(ElasticMaster.this.slaves);
//        executor.execute(ElasticMaster.this.clusterExecutorThread);
//        executor.shutdown();
//    }
//
//    private void extraSlavesDiscovery() throws IOException {
//
//        WorkerThreadFactory workerThreadFactory = new WorkerThreadFactory("SupportExecutor", false);
//            /* waiting further slaves */
//        while (ElasticMaster.this.isRunning()) {
//            while (ElasticMaster.this.state.getCurrentMainDistStage() != null && ElasticMaster.this.state.getCurrentMainDistStage().isScalable()) {
//                List<SocketChannel> tmpSlaves = new ArrayList<SocketChannel>(); // variabile che contiene i nuovi slaves da passare al nuovo thread
//                log.info("Waiting for other slaves to register to the master.");
//                discoverySelector.select();
//                Set<SelectionKey> keySet = discoverySelector.selectedKeys();
//                Iterator<SelectionKey> it = keySet.iterator();
//                while (it.hasNext()) {                                       //Seleziono solamente le chiavi con OP_ACCEPT
//                    SelectionKey selectionKey = it.next();
//                    it.remove();
//                    if (!selectionKey.isValid()) {
//                        continue;
//                    }
//                    log.info("*---*---* NEW SLAVE DISCOVERED *---*---*");
//                    ServerSocketChannel srvSocketChannel = (ServerSocketChannel) selectionKey.channel();
//                    SocketChannel socketChannel = srvSocketChannel.accept();
//                    ElasticMaster.this.slaves.add(socketChannel);      /* aggiungo sia alla variabile che contiene tutti gli slave*/
//                    tmpSlaves.add(socketChannel);   /* sia alla variabile temporanea che contiene un sott'insieme */
//                    slave2Index.put(socketChannel, (ElasticMaster.this.slaves.size() - 1));
//                    ElasticMaster.this.readBufferMap.put(socketChannel, ByteBuffer.allocate(DEFAULT_READ_BUFF_CAPACITY));
//                    log.trace("Discovery: readBufferMap size: " + ElasticMaster.this.readBufferMap.size());
//                    if (log.isTraceEnabled())
//                        log.trace("Added new slave connection from: " + socketChannel.socket().getInetAddress());
//                }
//                log.info("Connection established from " + tmpSlaves.size() + " new slaves.");
//                ExecutorService executor = Executors.newSingleThreadExecutor(workerThreadFactory);
//                Runnable worker = new SupportExecutor(new ArrayList<SocketChannel>(tmpSlaves));
//                executor.execute(worker);
//                executor.shutdown();
//            }
//        }
//    }
//
//}
//
//
//}
