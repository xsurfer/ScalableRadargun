package org.radargun.stages;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.DistStage;
import org.radargun.DistStageAck;
import org.radargun.config.MasterConfig;
import org.radargun.state.MasterState;
import org.radargun.state.SlaveState;
import org.radargun.utils.Utils;

import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;

/**
 * Support class for distributed mandatoryStages.
 *
 * @author Mircea.Markus@jboss.com
 */
public abstract class AbstractDistStage implements DistStage {

    protected Log log = LogFactory.getLog(getClass());

    protected String id;
    public String getId(){ return id; }
    public void setId(String id){ this.id = id; }

    private boolean scalable = false;
    public boolean isScalable() { return scalable; }
    public void setScalable(boolean scalable) { this.scalable = scalable; }

    protected boolean skippable = false;
    public boolean isSkippable() { return skippable; }
    public void setSkippable(boolean skippable) { this.skippable = skippable; }

    protected transient SlaveState slaveState;

    protected transient MasterConfig masterConfig;

    protected boolean exitBenchmarkOnSlaveFailure = false;

    protected int slaveIndex;

    private int activeSlavesCount;

    private int totalSlavesCount;

    private boolean runOnAllSlaves;

    public void initOnSlave(SlaveState slaveState) {
        this.slaveState = slaveState;
    }

    public void initOnMaster(MasterState masterState, int slaveIndex) {
        this.masterConfig = masterState.getConfig();
        this.slaveIndex = slaveIndex;
        assert masterConfig != null;
        this.totalSlavesCount = masterState.getConfig().getSlaveCount();
        if (isRunOnAllSlaves()) {
            log.debug(Thread.currentThread().getName() + " - " + "from initOnMaster: setActiveSlavesCount" );
            setActiveSlavesCount(totalSlavesCount);
        }
    }


    public void setRunOnAllSlaves(boolean runOnAllSlaves) {
        this.runOnAllSlaves = runOnAllSlaves;
    }

    public boolean isRunOnAllSlaves() {
        return runOnAllSlaves;
    }

    public boolean isExitBenchmarkOnSlaveFailure() {
        return exitBenchmarkOnSlaveFailure;
    }

    public void setExitBenchmarkOnSlaveFailure(boolean exitOnFailure) {
        this.exitBenchmarkOnSlaveFailure = exitOnFailure;
    }

    protected DefaultDistStageAck newDefaultStageAck(String className) {
        return new DefaultDistStageAck(getSlaveIndex(), slaveState.getLocalAddress(), className);
    }

    public DistStage clone() {
        try {
            return (DistStage) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new IllegalStateException(e);
        }
    }

    public boolean processAckOnMaster(List<DistStageAck> acks, MasterState masterState) {
        boolean success = true;
        logDurationInfo(acks);
        for (DistStageAck stageAck : acks) {
            DefaultDistStageAck defaultStageAck = (DefaultDistStageAck) stageAck;
            if (defaultStageAck.isError()) {
                log.warn(Thread.currentThread().getName() + " - " + "Received error ack " + defaultStageAck );
                return false;
            } else {
                log.trace(Thread.currentThread().getName() + " - " + "Received success ack " + defaultStageAck );
            }
        }
        if (log.isTraceEnabled())
            log.trace(Thread.currentThread().getName() + " - " + "All ack messages were successful" );
        return success;
    }

    /**
     * This method iterates acks list looking for nodes stopped by JMX.<br/>
     * @param acks Acks from previous stage
     * @param slaves All the slaves actually running the test
     * @return List of slaveIndex stopped by JMX
     */
    public List<Integer> sizeForNextStage(List<DistStageAck> acks, List<SocketChannel> slaves){
        if(acks.size() != slaves.size())
            log.warn("Number of acks and number of slaves SHOULD be ugual. It has to be different just in case SupportExecutor is running");
        return new ArrayList<Integer>();
    }

    protected void logDurationInfo(List<DistStageAck> acks) {
        if (!log.isInfoEnabled()) return;

        String processingDuration = "Durations [";
        boolean first = true;
        for (DistStageAck ack : acks) {
            if (first) first = false;
            else processingDuration += ", ";
            processingDuration += ack.getSlaveIndex() + ":" + Utils.prettyPrintMillis(ack.getDuration());
        }
        log.info(Thread.currentThread().getName() + " - " + "Received responses from all " + acks.size() + " slaves. " + processingDuration + "]" );
    }

    /**
     * Just in case of error, pasted during merging by Fabio
     * +    protected void logDurationInfo(List<DistStageAck> acks) {
     * +        if (!log.isInfoEnabled()) return;
     * +
     * +        Map<Integer, String> data = new TreeMap<Integer, String>();  // make sure this is sorted
     * +        for (DistStageAck dsa : acks){
     * +            data.put(dsa.getSlaveIndex(), Utils.prettyPrintTime(dsa.getDuration()));
     * +        }
     * +
     * +        String processingDuration = "Durations [";
     * +        boolean first = true;
     * +        for (Map.Entry<Integer, String> e : data.entrySet()) {
     * +            if (first) first = false;
     * +            else processingDuration += ", ";
     * +            processingDuration += e.getKey() + ":" + e.getValue();
     * +        }
     * +        log.info(Thread.currentThread().getName() + " - " + getClass().getSimpleName() + " received ack from all (" + acks.size() + ") slaves. " + processingDuration + "]" );
     * +    }
     */

    public int getActiveSlaveCount() {
        return activeSlavesCount;
    }

    public void setActiveSlavesCount(int activeSlaves) {
        log.debug(Thread.currentThread().getName() + " - " + "AbstractDistStage.setActiveSlavesCount - da " + this.activeSlavesCount + " a " + activeSlaves );
        this.activeSlavesCount = activeSlaves;
    }

//    public int getActiveScalingSlavesCount() {
//        return activeScalingSlavesCount;
//    }
//
//    public void setActiveScalingSlavesCount(int activeSlaves) {
//        log.debug(Thread.currentThread().getName() + " - " + "AbstractDistStage.setActiveScalingSlavesCount - da " + this.activeScalingSlavesCount + " a " + activeSlaves );
//        this.activeScalingSlavesCount = activeSlaves;
//    }

    public int getSlaveIndex() {
        return slaveIndex;
    }

    @Override
    public String toString() {
        return "slaveIndex=" + slaveIndex +
                ", activeSlavesCount=" + activeSlavesCount +
                ", totalSlavesCount=" + totalSlavesCount +
                "} ";
    }
}
