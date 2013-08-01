package org.radargun;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.config.MasterConfig;
import org.radargun.state.MasterState;
import org.radargun.utils.WorkerThreadFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.*;

/**
 * Author: Fabio Perfetti (perfabio87 [at] gmail.com)
 * Date: 8/1/13
 * Time: 2:46 PM
 */
public class NewElasticMaster {

    private static Log log = LogFactory.getLog(NewElasticMaster.class);
    public static final int DEFAULT_PORT = 2103;

    private MasterConfig masterConfig;
    private volatile MasterState state;

    private ServerSocketChannel serverSocketChannel;

    private Discoverer discoverer;
    private ClusterExecutor clusterExecutor;

    private CountDownLatch clusterCountDownLatch;

    public NewElasticMaster(MasterConfig masterConfig) {

        this.masterConfig = masterConfig;
        state = new MasterState(masterConfig);

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
            //prepareNextStage();
            //startCommunicationWithSlaves();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            releaseResources();
        }
    }

    private void startClusterExecutor(){
        clusterExecutor = new ClusterExecutor(state);
        ExecutorService service = Executors.newSingleThreadExecutor(new WorkerThreadFactory("ClusterExecutor", true));

        Future<Boolean> future = service.submit(clusterExecutor);
        service.shutdown();

        try {
            future.get();           // wait here while benchmark doesn't finish!!
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public void slaveJoined(SlaveSocketChannel socketChannel){
        if( !clusterExecutor.isRunning() ){
            log.info("Slave added to ClusterExecutor");
            clusterExecutor.addSlave(socketChannel);
        } else if( clusterExecutor.isReadyToScale() ) {
            // lancia un nuovo supportExecutor
            log.info("Slave need to be added to a SupportExecutor");


            int newSize = state.getCurrentMainDistStage().getActiveSlaveCount() + 1;
            log.info( "newSize: " + newSize );

            log.info("Updating current benchmark state");

            log.debug("Editing state.getCurrentMainDistStage().getActiveSlaveCount(): from " + state.getCurrentMainDistStage().getActiveSlaveCount() + " to " + newSize);
            state.getCurrentMainDistStage().setActiveSlavesCount(newSize);

            log.debug("Editing state.getCurrentBenchmark().currentFixedBenchmark().getSize: from " + state.getCurrentBenchmark().currentFixedBenchmark().getSize() + " to " + newSize);
            state.getCurrentBenchmark().currentFixedBenchmark().setSize(newSize);


        } else {
            try {
                socketChannel.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
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
