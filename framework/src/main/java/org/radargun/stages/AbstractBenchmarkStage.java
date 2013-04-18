package org.radargun.stages;

import org.radargun.CacheWrapper;
import org.radargun.DistStage;
import org.radargun.DistStageAck;
import org.radargun.jmx.annotations.ManagedAttribute;
import org.radargun.jmx.annotations.ManagedOperation;
import org.radargun.state.MasterState;
import org.radargun.stressors.AbstractBenchmarkStressor;
import org.radargun.workloadGenerator.AbstractWorkloadGenerator;

import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.lang.Double.parseDouble;
import static org.radargun.utils.Utils.numberFormat;

/**
 * Created by: Fabio Perfetti
 * E-mail: perfabio87@gmail.com
 * Date: 3/23/13
 */
public abstract class AbstractBenchmarkStage<T extends AbstractBenchmarkStressor> extends AbstractDistStage {

    /* ***************** */
    /* ** ATTRIBUTES *** */
    /* ***************** */

    /**
     * the number of threads that will work on this slave
     */
    protected int numOfThreads = 10;

    /**
     * total time (in seconds) of simulation for each stressor thread
     */
    protected long perThreadSimulTime = 180L;

    /**
     * timestamps (in ms) when this stage was starting
     */
    protected long initTimeStamp = 0L;

    /**
     * specify the interval period (in milliseconds) of the memory and cpu usage is collected
     */
    protected long statsSamplingInterval = 0;

    /**
     * the workload generator
     */
    protected AbstractWorkloadGenerator workloadGenerator;

    /* istanza di AbstractBenchmarkStressor */
    protected transient T stressor;

    protected transient CacheWrapper cacheWrapper;

    /**
     * If true, a transaction t is regenerated until it commits, unless it throws a "NotSuchElementException"
     * In this case, the transaction is aborted for good.
     */
    protected boolean retryOnAbort = false;

    protected boolean retrySameXact = false;

    /**
     * Specifies the msec a transaction spends in backoff after aborting
     */
    protected long backOffTime = 0;

    protected static final String SIZE_INFO = "SIZE_INFO";



    /* ****************** */
    /* ** TO OVERRIDE *** */
    /* ****************** */



    /* *************** */
    /* *** METHODS *** */
    /* *************** */

    public void initOnMaster(MasterState masterState, int slaveIndex) {
        super.initOnMaster(masterState, slaveIndex);
        this.setInitTimeStamp();
    }


    public void updateTimes(DistStage currentMainStage) {
        log.info("Updating perThreadSimulTime");

        long totalSimulTime = ((AbstractBenchmarkStage) currentMainStage).getPerThreadSimulTime();
        long currentMainStageInitTs = ((AbstractBenchmarkStage) currentMainStage).getInitTimeStamp();
        long toExecuteInitTs = this.getInitTimeStamp();
        long elapsedTimeFromBeginning = toExecuteInitTs - currentMainStageInitTs;
        long secondToExecute = totalSimulTime - (elapsedTimeFromBeginning / 1000);
        if (secondToExecute < 0) { secondToExecute = 0; }

        log.info("This stage will execute for: " + secondToExecute);
        this.setPerThreadSimulTime(secondToExecute);

        log.info("Updating initTime Workload Generator");

        this.getWorkloadGenerator().setInitTime( (int) (elapsedTimeFromBeginning / 1000) );
    }


    /**
     * This method iterates acks list looking for nodes stopped by JMX.<br/>
     * @param acks Acks from previous stage
     * @param slaves All the slaves actually running the test
     * @return List of slaveIndex stopped by JMX
     */
    public List<Integer> sizeForNextStage(List<DistStageAck> acks, List<SocketChannel> slaves){
        List<Integer> ret = new ArrayList<Integer>();

        if(acks.size() != slaves.size())
            throw new IllegalStateException("Number of acks and number of slaves MUST be ugual");

        for (DistStageAck ack : acks) {
            DefaultDistStageAck wAck = (DefaultDistStageAck) ack;
            Map<String, Object> benchResult = (Map<String, Object>) wAck.getPayload();
            if (benchResult != null) {
                Object stoppedByJmxObj = benchResult.get("STOPPED");
                if (stoppedByJmxObj == null) {
                    throw new IllegalStateException("STOPPED should be there!");
                }
                boolean stoppedByJmx = Boolean.parseBoolean(stoppedByJmxObj.toString());
                if(stoppedByJmx){
                    log.info("Slave " + ack.getSlaveIndex() + " has been stopped via JMX, I don't use it anymore");
                    ret.add(new Integer(ack.getSlaveIndex()));
                }
            } else {
                log.warn("No report received from slave: " + ack.getSlaveIndex());
            }
        }

        log.info("Next stage will start with: " + (slaves.size() - ret.size()) + " slaves");
        return ret;
    }

    public AbstractBenchmarkStage clone() {
        AbstractBenchmarkStage clone = (AbstractBenchmarkStage) super.clone();
        log.info("cloning AbstractBenchmarkStage");
        clone.initTimeStamp = 0;
        clone.workloadGenerator = workloadGenerator.clone();

        return clone;
    }

    @Override
    public boolean processAckOnMaster(List<DistStageAck> acks, MasterState masterState) {
        logDurationInfo(acks);
        boolean success = true;
        Map<Integer, Map<String, Object>> results = new HashMap<Integer, Map<String, Object>>();
        masterState.put("results", results);
        for (DistStageAck ack : acks) {
            DefaultDistStageAck wAck = (DefaultDistStageAck) ack;
            if (wAck.isError()) {
                success = false;
                log.warn("Received error ack: " + wAck);
            } else {
                if (log.isTraceEnabled())
                    log.trace(wAck);
            }
            Map<String, Object> benchResult = (Map<String, Object>) wAck.getPayload();
            if (benchResult != null) {
                results.put(ack.getSlaveIndex(), benchResult);
                Object reqPerSes = benchResult.get("REQ_PER_SEC");
                if (reqPerSes == null) {
                    throw new IllegalStateException("This should be there!");
                }
                log.info("On slave " + ack.getSlaveIndex() + " we had " + numberFormat(parseDouble(reqPerSes.toString())) + " requests per second");
                log.info("Received " + benchResult.remove(SIZE_INFO));
            } else {
                log.trace("No report received from slave: " + ack.getSlaveIndex());
            }
        }
        return success;
    }


    /* ******************************* */
    /* *** MANAGED METHODS VIA JMX *** */
    /* ******************************* */

    @ManagedAttribute(description = "Returns the number of threads created", writable = false)
    public final int getNumOfThreads() {
        return stressor.getNumberOfThreads();
    }

    @ManagedAttribute(description = "Returns the number of threads actually running", writable = false)
    public final int getNumberOfActiveThreads() {
        return stressor.getNumberOfActiveThreads();
    }

    @ManagedOperation(description = "Change the number of threads running, creating more threads if needed")
    public final void setNumberOfActiveThreads(int numberOfActiveThreads) {
        stressor.setNumberOfRunningThreads(numberOfActiveThreads);
    }

    @ManagedOperation(description = "Stop the current benchmark")
    public final void stopBenchmark() {
        stressor.stopBenchmark();
    }




    /* ***********************/
    /* *** GETTER/SETTER *** */
    /* ********************* */

    public void setNumOfThreads(int numOfThreads) { this.numOfThreads = numOfThreads; }

    public void setBackOffTime(long backOffTime) {
        this.backOffTime = backOffTime;
    }

    public void setRetryOnAbort(boolean retryOnAbort) {
        this.retryOnAbort = retryOnAbort;
    }

    public void setRetrySameXact(boolean b){
        this.retrySameXact = b;
    }

    public void setStatsSamplingInterval(long statsSamplingInterval) { this.statsSamplingInterval = statsSamplingInterval; }

    public void setWorkloadGenerator(AbstractWorkloadGenerator wg){ this.workloadGenerator = wg; }
    public AbstractWorkloadGenerator getWorkloadGenerator(){ return this.workloadGenerator; }

    public long getPerThreadSimulTime(){ return this.perThreadSimulTime; }
    public void setPerThreadSimulTime(long perThreadSimulTime){ this.perThreadSimulTime = perThreadSimulTime; }

    public long getInitTimeStamp() { return this.initTimeStamp; }
    public void setInitTimeStamp() { this.initTimeStamp = System.currentTimeMillis(); log.info("SETTING initTimeStamp to: " + initTimeStamp); }

    public T getStressor(){ return stressor; }

}
