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

    public StressorParameter(CountDownLatch startPoint){
        this.startPoint = startPoint;
    }


    /* ****************** */
    /* *** ATTRIBUTES *** */
    /* ****************** */

    protected volatile CountDownLatch startPoint;

    /**
     * total time (in seconds) of simulation for each stressor thread
     */
    protected long perThreadSimulTime = 30L;

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
    protected boolean retryOnAbort = false;

    protected boolean retrySameXact = false;

    protected CacheWrapper cacheWrapper;

    protected boolean stoppedByJmx = false;



    /* *************** */
    /* *** GETTERS *** */
    /* *************** */

    public CountDownLatch getStartPoint() { return startPoint; }

    public long getPerThreadSimulTime() { return perThreadSimulTime; }

    public int getNumOfThreads() { return numOfThreads; }

    public int getNodeIndex() { return nodeIndex; }

    public int getNumSlaves() { return numSlaves; }

    public long getBackOffTime() { return backOffTime; }

    public boolean isRetryOnAbort() { return retryOnAbort; }

    public boolean isRetrySameXact() { return retrySameXact; }

    public CacheWrapper getCacheWrapper() { return cacheWrapper; }

    public boolean isStoppedByJmx() { return stoppedByJmx; }

}
