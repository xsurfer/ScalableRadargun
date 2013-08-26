package org.radargun.stages.stressors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.CacheWrapper;
import org.radargun.stages.synthetic.XACT_RETRY;

import java.util.concurrent.CountDownLatch;

/**
 * Created by: Fabio Perfetti
 * E-mail: perfabio87@gmail.com
 * Date: 4/19/13
 */
public class Parameters {

    private static Log log = LogFactory.getLog(Parameters.class);

    /* ******************* */
    /* *** CONSTRUCTOR *** */
    /* ******************* */

    public Parameters(CacheWrapper cacheWrapper,
                      long simulationTimeSec,
                      int numOfThreads,
                      int nodeIndex,
                      long backOffTime,
                      XACT_RETRY retryOnAbort,
                      long statsSamplingInterval ){
        this.cacheWrapper = cacheWrapper;
        this.simulationTimeSec = simulationTimeSec;
        this.numOfThreads = numOfThreads;
        this.nodeIndex = nodeIndex;
        this.backOffTime = backOffTime;
        this.retryOnAbort = retryOnAbort;
        this.statsSamplingInterval = statsSamplingInterval;
    }


    /* ****************** */
    /* *** ATTRIBUTES *** */
    /* ****************** */

    protected final CacheWrapper cacheWrapper;

    protected CountDownLatch startPoint;

    /**
     * total time (in seconds) of simulation for each stressor thread
     */
    protected final long simulationTimeSec;

    /**
     * the number of threads that will work on this cache wrapper.
     */
    protected final int numOfThreads;

    /**
     * this node's index in the Radargun cluster.  -1 is used for local benchmarks.
     */
    protected final int nodeIndex;

    /**
     * the number of nodes in the Radargun cluster.
     * BE CAREFUL, in this version of radargun this number have to be dynamic, so read the value in the cache
     */
    protected final int numSlaves = -1;

    /**
     * Specifies the msec a transaction spends in backoff after aborting
     */
    protected final long backOffTime;

    /**
     * If true, after the abort of a transaction t of type T, a new transaction t' of type T is generated
     */
    protected final XACT_RETRY retryOnAbort;// = XACT_RETRY.NO_RETRY;

    /**
     * specify the interval period (in milliseconds) of the memory and cpu usage is collected
     */
    protected final long statsSamplingInterval;


    /* *************** */
    /* *** GETTERS *** */
    /* *************** */

    public CacheWrapper getCacheWrapper() { return cacheWrapper; }
    //public void setCacheWrapper(CacheWrapper cacheWrapper) { this.cacheWrapper = cacheWrapper; }

    public int getNodeIndex() { return nodeIndex; }
    //public void setNodeIndex(int val) { nodeIndex = val; }

    public CountDownLatch getStartPoint() { return startPoint; }
    public void setStartPoint(CountDownLatch val) { startPoint = val; }

    public long getSimulationTimeSec() { return simulationTimeSec; }
    //public void setSimulationTimeSec(long val) { simulationTimeSec =val; }

    public int getNumOfThreads() { return numOfThreads; }
    //public void setNumOfThreads( int val ) { numOfThreads=val; }

    public int getNumSlaves() {
        int numSlaves = cacheWrapper.getNumMembers();
        log.info("NumSlaves: " + numSlaves);
        return numSlaves;

        //throw new RuntimeException("TO FIX, the number of slave is dynamic, read it from the cache");
        //return numSlaves;
    }

    public void setNumSlaves(int val) {
        throw new RuntimeException("TO FIX, the number of slave is dynamic, read it from the cache");
        //numSlaves = val;
    }

    public long getBackOffTime() { return backOffTime; }
    //public void setBackOffTime(long val) { backOffTime = val; }

    public XACT_RETRY getRetryOnAbort(){
        return retryOnAbort;
    }
    //public void setRetryOnAbort(XACT_RETRY val) { retryOnAbort = val; }

    public long getStatsSamplingInterval() { return statsSamplingInterval; }
    //public void setStatsSamplingInterval(long statsSamplingInterval) { this.statsSamplingInterval = statsSamplingInterval; }


//    public boolean isStoppedByJmx() { return stoppedByJmx; }

    public boolean isRetryOnAbort(){
        return !retryOnAbort.equals(XACT_RETRY.NO_RETRY);
    }
}
