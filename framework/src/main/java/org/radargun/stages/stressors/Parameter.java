package org.radargun.stages.stressors;

import org.radargun.CacheWrapper;
import org.radargun.stages.synthetic.XACT_RETRY;

import java.util.concurrent.CountDownLatch;

/**
 * Created by: Fabio Perfetti
 * E-mail: perfabio87@gmail.com
 * Date: 4/19/13
 */
public class Parameter {

    /* ******************* */
    /* *** CONSTRUCTOR *** */
    /* ******************* */

    public Parameter(){

    }


    /* ****************** */
    /* *** ATTRIBUTES *** */
    /* ****************** */

    protected CacheWrapper cacheWrapper;

    protected volatile CountDownLatch startPoint;

    /**
     * total time (in seconds) of simulation for each stressor thread
     */
    protected long simulationTimeSec = 30L;

    /**
     * the number of threads that will work on this cache wrapper.
     */
    protected int numOfThreads = 10;

    /**
     * this node's index in the Radargun cluster.  -1 is used for local benchmarks.
     */
    protected int nodeIndex = -1;

    /**
     * the number of nodes in the Radargun cluster.
     */
    protected int numSlaves = 0;

    /**
     * Specifies the msec a transaction spends in backoff after aborting
     */
    protected long backOffTime = 0;

    /**
     * If true, after the abort of a transaction t of type T, a new transaction t' of type T is generated
     */
    protected XACT_RETRY retryOnAbort = XACT_RETRY.NO_RETRY;

    /**
     * specify the interval period (in milliseconds) of the memory and cpu usage is collected
     */
    protected long statsSamplingInterval = 0;


    /* *************** */
    /* *** GETTERS *** */
    /* *************** */

    public CacheWrapper getCacheWrapper() { return cacheWrapper; }
    public void setCacheWrapper(CacheWrapper cacheWrapper) { this.cacheWrapper = cacheWrapper; }

    public int getNodeIndex() { return nodeIndex; }
    public void setNodeIndex(int val) { nodeIndex = val; }

    public CountDownLatch getStartPoint() { return startPoint; }
    public void setStartPoint(CountDownLatch val) { startPoint = val; }

    public long getSimulationTimeSec() { return simulationTimeSec; }
    public void setSimulationTimeSec(long val) { simulationTimeSec =val; }

    public int getNumOfThreads() { return numOfThreads; }
    public void setNumOfThreads( int val ) { numOfThreads=val; }

    public int getNumSlaves() { return numSlaves; }
    public void setNumSlaves(int val) { numSlaves = val; }

    public long getBackOffTime() { return backOffTime; }
    public void setBackOffTime(long val) { backOffTime = val; }

    public XACT_RETRY getRetryOnAbort(){
        return retryOnAbort;
    }

    public void setRetryOnAbort(XACT_RETRY val) {
        retryOnAbort = val;
    }

    public long getStatsSamplingInterval() { return statsSamplingInterval; }
    public void setStatsSamplingInterval(long statsSamplingInterval) { this.statsSamplingInterval = statsSamplingInterval; }


//    public boolean isStoppedByJmx() { return stoppedByJmx; }

    public boolean isRetryOnAbort(){
        return !retryOnAbort.equals(XACT_RETRY.NO_RETRY);
    }
}
