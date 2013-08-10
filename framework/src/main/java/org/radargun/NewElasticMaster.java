package org.radargun;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.config.MasterConfig;
import org.radargun.state.MasterState;
import org.radargun.utils.WorkerThreadFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Author: Fabio Perfetti (perfabio87 [at] gmail.com)
 * Date: 8/1/13
 * Time: 2:46 PM
 */
public class NewElasticMaster {

    private static Log log = LogFactory.getLog(NewElasticMaster.class);

    public static final int DEFAULT_PORT = 2103;

    private MasterConfig masterConfig;

    private ServerSocketChannel serverSocketChannel;

    private Discoverer discoverer;

    private ClusterExecutor clusterExecutor;

    private ExecutorService clusterExecutorService = Executors.newSingleThreadExecutor(new WorkerThreadFactory("ClusterExecutor", true));

    private WorkerThreadFactory supportExecutorThreadFactory = new WorkerThreadFactory("SupportExecutor", false);

    private AtomicBoolean clusterExecutorStarted = new AtomicBoolean(false);
    private CountDownLatch clusterCountDownLatch;

    private final Set<SlaveSocketChannel> slaves = Collections.synchronizedSet(new HashSet<SlaveSocketChannel>());



    public NewElasticMaster(MasterConfig masterConfig) {
        this.masterConfig = masterConfig;
        setShutDownHook();
    }

    protected void setShutDownHook(){
        Runtime.getRuntime().addShutdownHook(new ShutDownHook("NewElasticMaster process"));
    }

    public void start() {
        try {
            clusterCountDownLatch = new CountDownLatch( masterConfig.getSlaveCount() );
            startServerSocket();
            discoverer = new Discoverer(this, serverSocketChannel);
            discoverer.start(); //runDiscovery();

            startClusterExecutor();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            releaseResources();
        }
    }

    private void startClusterExecutor(){
        try {
            clusterCountDownLatch.await();
        } catch (InterruptedException e) {
            log.warn(e,e);  //ignore
        }
        clusterExecutorStarted.compareAndSet(false, true);

        // 1) create the clusterExecutor
        clusterExecutor = new ClusterExecutor(new MasterState(masterConfig), new HashSet<SlaveSocketChannel>(slaves) );

        // 2) empty slaves
        slaves.clear();

        // 3) start the clusterExecutor

        Future<Boolean> future = clusterExecutorService.submit(clusterExecutor);
        clusterExecutorService.shutdown();

        // 4) wait till cluster executor doesn't finish
        try {
            future.get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public void slaveJoined(SlaveSocketChannel slaveSocketChannel){

        slaves.add(slaveSocketChannel);

        if(!clusterExecutorStarted.get()){
            clusterCountDownLatch.countDown();
        } else {
            log.warn("Logica qui per tirare su un support executor");
            synchronized (slaves){
                for (SlaveSocketChannel scc : slaves){
                    clusterExecutor.addSlave(scc);
                }

                ExecutorService supportExecutorService = Executors.newSingleThreadExecutor(supportExecutorThreadFactory);
                supportExecutorService.submit( new SupportExecutor( new MasterState(masterConfig), new HashSet<SlaveSocketChannel>(slaves), clusterExecutor ) );
                supportExecutorService.shutdown();

                slaves.clear();
            }
            log.warn("Fuori dalla sync");

        }
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

    private void releaseResources() {

        discoverer.stop(true);

//        try {
//            communicationSelector.close();
//        } catch (Throwable e) {
//            log.warn(e);
//        }
//        for (SocketChannel sc : slaves) {
//            try {
//                sc.socket().close();
//            } catch (Throwable e) {
//                log.warn(e);
//            }
//        }
//
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
