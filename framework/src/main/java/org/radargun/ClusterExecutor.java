package org.radargun;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.state.MasterState;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.util.HashSet;
import java.util.Set;

/**
 * Author: Fabio Perfetti (perfabio87 [at] gmail.com)
 * Date: 8/1/13
 * Time: 2:47 PM
 */
public class ClusterExecutor extends AbstractExecutor {

    private static Log log = LogFactory.getLog(ClusterExecutor.class);

    private Set<SlaveSocketChannel> joiningSlaves = new HashSet<SlaveSocketChannel>();
    private Set<SlaveSocketChannel> stoppedSlaves = new HashSet<SlaveSocketChannel>();

    public ClusterExecutor(MasterState state, Set<SlaveSocketChannel> slaves) {
        super(state, slaves);
    }

    @Override
    protected boolean assertRunning() {
        return true;
    }

    @Override
    protected void postStageBroadcast() {
        // nothing to do
    }

    @Override
    protected int numAckToNextStage() {
        return slaves.size() + joiningSlaves.size() + stoppedSlaves.size();
    }

    @Override
    protected void slaveStopped(SlaveSocketChannel slave) {
        int newSize = state.getCurrentDistStage().getActiveSlaveCount() - 1;

        log.debug("Editing state.getCurrentMainDistStage().getActiveSlaveCount(): from " + state.getCurrentDistStage().getActiveSlaveCount() + " to " + newSize);
        state.getCurrentDistStage().setActiveSlavesCount(newSize);

        log.debug("Editing state.getCurrentBenchmark().currentFixedBenchmark().getSize: from " + state.getCurrentBenchmark().currentFixedBenchmark().getSize() + " to " + newSize);
        state.getCurrentBenchmark().currentFixedBenchmark().setSize(newSize);

        if( !slaves.remove(slave) ){
            if( joiningSlaves.remove(slave) ){
                log.debug("Joining slave deleted");
            }
        } else {
            log.debug("Slave deleted");
            stoppedSlaves.add(slave);
        }

        log.info("Number of working slaves: " + slaves.size() );
        log.info("Number of joining slaves: " + joiningSlaves.size() );
        log.info("Number of stopped slaves: " + stoppedSlaves.size() );
    }

    @Override
    protected void preSerialization(DistStage readyToWriteOnBuffer) {
        // nothing to do here
    }

    @Override
    protected void post() {
        // nothing to do
    }

    public DistStage currentDistStage(){
        return state.getCurrentDistStage();
    }

    public void addSlave(SlaveSocketChannel slavesReadyToMerge){

        int newSize = state.getCurrentDistStage().getActiveSlaveCount() + 1;

        log.debug("Editing state.getCurrentMainDistStage().getActiveSlaveCount(): from " + state.getCurrentDistStage().getActiveSlaveCount() + " to " + newSize);
        state.getCurrentDistStage().setActiveSlavesCount(newSize);

        log.debug("Editing state.getCurrentBenchmark().currentFixedBenchmark().getSize: from " + state.getCurrentBenchmark().currentFixedBenchmark().getSize() + " to " + newSize);
        state.getCurrentBenchmark().currentFixedBenchmark().setSize(newSize);

        joiningSlaves.add(slavesReadyToMerge);
    }

    public void mergeSlave(SlaveSocketChannel slaveReadyToMerge){

        if( joiningSlaves.contains(slaveReadyToMerge) ){
            // ************************** This code should be executed atomically *******************
            log.trace("Merging slave id: " + slaveReadyToMerge.getId());
            slaves.add(slaveReadyToMerge);
            joiningSlaves.remove(slaveReadyToMerge);
            initBuffer( slaveReadyToMerge.getSocketChannel() );
            // ************************** This code should be executed atomically *******************

        } else {
            log.error("Trying to merge a not joining slave!!!");
            throw new RuntimeException("Trying to merge a not joining slave!!!");
        }

        try {
            slaveReadyToMerge.getSocketChannel().configureBlocking(false);
            slaveReadyToMerge.getSocketChannel().register(communicationSelector, SelectionKey.OP_READ);
        } catch (IOException e) {
            log.error(e,e);
            throw new RuntimeException(e);
        }

        changeNumSlavesNotify();
    }

    private void changeNumSlavesNotify(){
        log.info("Notifying all the slaves to change numNodes");

        for(SlaveSocketChannel slave : slaves){
            String host = slave.getSocketChannel().socket().getInetAddress().getHostName();
            String currentBenchmark = state.getCurrentDistStage().getClass().getName();

            int startIndex = currentBenchmark.lastIndexOf(".")+1;
            int lastIndex =  currentBenchmark.length()-new String("Stage").length();
            currentBenchmark = currentBenchmark.substring(startIndex,lastIndex);

            log.info("CurrentStage: " + currentBenchmark);
            NumNodesJmxRequest jmxRequest = new NumNodesJmxRequest(currentBenchmark, host, NumNodesJmxRequest.DEFAULT_JMX_PORT);
            jmxRequest.doRequest();
        }
    }

}
