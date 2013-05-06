package org.radargun.stressors.consumer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.CacheWrapper;
import org.radargun.Transaction;
import org.radargun.stages.AbstractBenchmarkStage;
import org.radargun.stressors.BenchmarkStressor;
import org.radargun.stressors.StressorParameter;
import org.radargun.stressors.commons.StressorStats;
import org.radargun.stressors.exceptions.ApplicationException;
import org.radargun.stressors.producer.ProducerRate;
import org.radargun.stressors.producer.RequestType;
import org.radargun.workloadGenerator.AbstractWorkloadGenerator;
import org.radargun.workloadGenerator.SystemType;

/**
 * Created by: Fabio Perfetti
 * E-mail: perfabio87@gmail.com
 * Date: 4/24/13
 */
public abstract class Consumer<T extends SystemType> extends Thread {

    private static Log log = LogFactory.getLog(Consumer.class);

    protected CacheWrapper cacheWrapper;

    protected int threadIndex;

    protected T system;

    protected AbstractBenchmarkStage stage;

    protected BenchmarkStressor stressor;

    protected StressorParameter parameters;



    public StressorStats stats;

    protected ProducerRate backOffSleeper;

    protected boolean running = true;

    protected boolean active = true;

    /* ******************* */
    /* *** CONSTRUCTOR *** */
    /* ******************* */

    public Consumer(CacheWrapper cacheWrapper,
                    int threadIndex,
                    T system,
                    AbstractBenchmarkStage stage,
                    BenchmarkStressor stressor,
                    StressorParameter parameters) {

        super("Stressor-" + threadIndex);

        this.cacheWrapper = cacheWrapper;
        this.system = system;
        this.stage = stage;
        this.stressor = stressor;
        this.parameters = parameters;

        stats = new StressorStats();

        if (parameters.getBackOffTime() > 0)
            try {
                // TODO renderlo customizable
                this.backOffSleeper =
                        ProducerRate.createInstance(AbstractWorkloadGenerator.RateDistribution.EXPONENTIAL,
                                Math.pow((double) parameters.getBackOffTime(), -1D)
                        );
            } catch (ProducerRate.ProducerRateException e) {
                throw new RuntimeException(e);
            }
    }

    /* ******************* */
    /* *** TO OVERRIDE *** */
    /* ******************* */

    protected abstract void consume();



    /* *************** */
    /* *** METHODS *** */
    /* *************** */

    @Override
    public final void run() {
        // TODO refactoring!!!

        synchronize();
        consume();
    }


    protected void synchronize(){
        try {
            parameters.getStartPoint().await();
            log.info("Starting thread: " + getName());
        } catch (InterruptedException e) {
            log.warn("Interrupted while waiting for starting in " + getName());
        }
    }

    protected boolean processTransaction(Transaction tx) {
        // entrambi le fasi devono essere fatte!!!

        boolean successful = true;
        boolean isSafeToRetry = true;
        boolean localAbort = false;
        boolean remoteAbort = false;

        stats._handleStartsTx(tx);
        //TODO per diego, gestisciti CCTP i retry
        do {
            tx = regenerate(tx, threadIndex, successful);
            cacheWrapper.startTransaction();
            try {
                tx.executeTransaction(cacheWrapper);
                stats._handleSuccessLocalTx(tx);
                log.info("Thread " + threadIndex + " successfully completed locally a transaction of type " +
                        tx.getType() + " btw, successful is " + successful);

            } catch (Throwable e) {
                localAbort= true;
                successful = false;
                if (log.isDebugEnabled()) {
                    log.debug("Exception while executing transaction.", e);
                } else {
                    log.warn("Exception while executing transaction of type: " + tx.getType() + " " + e.getMessage());
                }

                if (e instanceof ApplicationException) {
                    isSafeToRetry = ( (ApplicationException) e ).allowsRetry();
                }

                stats._handleAbortLocalTx(tx,e, cacheWrapper.isTimeoutException(e));
            }

            if(!localAbort){
                stats._handleSuccessLocalTx(tx);
            }

            //here we try to finalize the transaction
            //if any read/write has failed we abort

            try {
                cacheWrapper.endTransaction(successful, threadIndex);
                if (successful) {
                    log.info("Thread " + threadIndex + " successfully completed remotely a transaction of type " +
                            tx.getType() + " Btw, successful is " + successful);
                }
            } catch (Throwable e) {
                // errore o in rollback o commit
                if( successful ){ // errore nel commit
                    remoteAbort=true;
                    successful = false;
                    stats._handleAbortRemoteTx(tx, e);
                }

                if (log.isDebugEnabled()) {
                    log.debug("Error while committing", e);
                } else {
                    log.warn("Error while committing: " + e.getMessage());
                }
            }

            if(!remoteAbort){
                stats._handleSuccessRemoteSuccessTx(tx);
            }

        }
        //If we experience an elementNotFoundException we do not want to restart the very same xact!!
        //If a xact is not progressing at the endTxTimestamp of the test, we kill it. Some stats will be slightly affected by this
        while (assertRunning() && parameters.isRetryOnAbort() && !successful && isSafeToRetry);

        return successful;
    }


    protected Transaction regenerate(Transaction oldTransaction, int threadIndex, boolean lastSuccessful) {

        if (!lastSuccessful && !parameters.isRetrySameXact()) {
            this.backoffIfNecessary();
            Transaction newTransaction = stage.generateTransaction(new RequestType(System.nanoTime(), oldTransaction.getType()), threadIndex);
            copyTimeStampInformation(oldTransaction, newTransaction);
            log.info("Thread " + threadIndex + ": regenerating a transaction of type " + oldTransaction.getType() +
                    " into a transaction of type " + newTransaction.getType());
            return newTransaction;
        }
        //If this is the first time xact runs or exact retry on abort is enabled...
        return oldTransaction;
    }


    protected void backoffIfNecessary() {
        if (parameters.getBackOffTime() != 0) {
            stats.inc(StressorStats.NUM_BACK_OFFS);
            long backedOff = backOffSleeper.sleep();
            log.info("Thread " + this.threadIndex + " backed off for " + backedOff + " msec");
            stats.put(StressorStats.BACKED_OFF_TIME, backedOff);
        }
    }


    protected void copyTimeStampInformation(Transaction oldTx, Transaction newTx){
        newTx.setEnqueueTimestamp(oldTx.getEnqueueTimestamp());
        newTx.setDequeueTimestamp(oldTx.getDequeueTimestamp());
        newTx.setStartTimestamp(oldTx.getStartTimestamp());
    }


    protected boolean startNewTransaction(boolean lastXactSuccessul) {
        return !parameters.isRetryOnAbort() || lastXactSuccessul;
    }

        /*
        public long totalDuration() {
            return readDuration + writeDuration;
        }
        */

    protected synchronized boolean assertRunning() {
        return running;
    }


    public final synchronized void inactive() {
        active = false;
    }


    public final synchronized void active() {
        active = true;
        notifyAll();
    }


    public final synchronized void finish() {
        active = true;
        running = false;
        notifyAll();
    }


    public final synchronized boolean isActive() {
        return active;
    }


    protected synchronized void blockIfInactive() {
        while (!active) {
            try {
                wait();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }


}
