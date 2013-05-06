package org.radargun.stressors;

import org.radargun.CacheWrapper;
import org.radargun.stressors.producer.RequestType;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;

/**
 * Created by: Fabio Perfetti
 * E-mail: perfabio87@gmail.com
 * Date: 4/19/13
 */
public class StressorParameter {

    /* ******************* */
    /* *** CONSTRUCTOR *** */
    /* ******************* */

    public StressorParameter(){

    }


    /* ****************** */
    /* *** ATTRIBUTES *** */
    /* ****************** */

    private volatile CountDownLatch startPoint;

    /**
     * total time (in seconds) of simulation for each stressor thread
     */
    private long perThreadSimulTime = 30L;

    /**
     * the number of threads that will work on this cache wrapper.
     */
    private int numOfThreads = 10;

    /**
     * this node's index in the Radargun cluster.  -1 is used for local benchmarks.
     */
    private int nodeIndex = -1;

    /**
     * the number of nodes in the Radargun cluster.
     */
    private int numSlaves = 0;

    /**
     * Specifies the msec a transaction spends in backoff after aborting
     */
    private long backOffTime = 0;

    /**
     * If true, after the abort of a transaction t of type T, a new transaction t' of type T is generated
     */
    private boolean retryOnAbort = false;

    private boolean retrySameXact = false;

    /**
     * specify the interval period (in milliseconds) of the memory and cpu usage is collected
     */
    private long statsSamplingInterval = 0;


    /* *************** */
    /* *** GETTERS *** */
    /* *************** */

    public int getNodeIndex() { return nodeIndex; }
    public void setNodeIndex(int val) { nodeIndex = val; }

    public CountDownLatch getStartPoint() { return startPoint; }

    public long getPerThreadSimulTime() { return perThreadSimulTime; }
    public void setPerThreadSimulTime( long val ) { perThreadSimulTime=val; }

    public int getNumOfThreads() { return numOfThreads; }
    public void setNumOfThreads( int val ) { numOfThreads=val; }

    public int getNumSlaves() { return numSlaves; }
    public void setNumSlaves(int val) { numSlaves = val; }

    public long getBackOffTime() { return backOffTime; }
    public void setBackOffTime(long val) { backOffTime = val; }

    public boolean isRetryOnAbort() { return retryOnAbort; }
    public void setRetryOnAbort(boolean val) { retryOnAbort = val; }

    public boolean isRetrySameXact() { return retrySameXact; }
    public void setRetrySameXact(boolean val) { retrySameXact = val; }

    public long getStatsSamplingInterval() { return statsSamplingInterval; }
    public void setStatsSamplingInterval(long statsSamplingInterval) { this.statsSamplingInterval = statsSamplingInterval; }


//    public boolean isStoppedByJmx() { return stoppedByJmx; }

}
