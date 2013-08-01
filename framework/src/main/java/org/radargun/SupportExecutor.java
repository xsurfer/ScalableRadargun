package org.radargun;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.stages.AbstractBenchmarkStage;
import org.radargun.state.MasterState;
import org.radargun.utils.WorkerThreadFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Author: Fabio Perfetti (perfabio87 [at] gmail.com)
 * Date: 8/1/13
 * Time: 6:44 PM
 */
public class SupportExecutor {

    private static Log log = LogFactory.getLog(SupportExecutor.class);

    private Selector communicationSelector;

    private List<SocketChannel> slaves;
    private ClusterExecutor clusterExecutor;
    private MasterState state;

    private Map<SocketChannel, ByteBuffer> writeBufferMap = new HashMap<SocketChannel, ByteBuffer>();
    private Map<SocketChannel, ByteBuffer> readBufferMap = new HashMap<SocketChannel, ByteBuffer>();

    public SupportExecutor(List<SocketChannel> slaves, MasterState state, ClusterExecutor clusterExecutor) {
        this.state = state;
        this.slaves = slaves;
        this.clusterExecutor = clusterExecutor;
    }


    @Override
    protected void finalize() {

        clusterExecutor.mergeSlave(slaves);

        //changeNumSlavesNotify();

        log.info("Sent data to master, killing!");
    }

    protected void runDistStage(DistStage currentStage) throws Exception {
        writeBufferMap.clear();

        log.trace("Sending current stage to: " + slaves.size() + " slaves");
        DistStage toSerialize;
        for(SocketChannel slave : slaves){
            // different way to retrieve the slave. This is most generic avoiding to reimplement it for SupportExecutor
            SocketChannel slave = slaves.get( slave2Index.get( slaves.get(i) ) );

            slave.configureBlocking(false);
            slave.register(communicationSelector, SelectionKey.OP_WRITE);
            toSerialize = currentStage.clone();
            toSerialize.initOnMaster(state, ((slaves.size() - localSlaves.size()) + i));
            preSerialization(toSerialize);
            if (i == 0) {//only log this once
                log.info("Starting dist stage '" + toSerialize.getClass().getSimpleName() + "' on " + noSlavesToSend + " Slaves: " + toSerialize);
            }
            byte[] bytes = SerializationHelper.prepareForSerialization(toSerialize);
            writeBufferMap.put(slave, ByteBuffer.wrap(bytes));
        }

    }

    protected void prepareNextStage() throws Exception {
        DistStage toExecute = null;

        // salta finchè non è raggiunto il current stage.
        // nel caso in cui lo stage non è skippable ma è settato come RunOnAllSlaves allora lancia una eccezione
        // se lo stage non è skippable e non è RunOnAllSlaves allora devo eseguirlo

        boolean isCurrent = false;
        do {
            toExecute = state.getNextDistStageToProcess();
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

        // using slaves.size it will send next stage only to new slaves
        runDistStage(toExecute);
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