package org.radargun.stressors.consumer;

import org.radargun.CacheWrapper;
import org.radargun.Transaction;
import org.radargun.stressors.StressorParameter;
import org.radargun.stressors.commons.StressorStats;
import org.radargun.stressors.exceptions.ApplicationException;
import org.radargun.stressors.producer.ProducerRate;
import org.radargun.stressors.producer.RequestType;
import org.radargun.workloadGenerator.AbstractWorkloadGenerator;

/**
 * Created by: Fabio Perfetti
 * E-mail: perfabio87@gmail.com
 * Date: 4/19/13
 */
public class Consumer extends Thread {
    protected int threadIndex;
    //private double arrivalRate;

    public long commit_start = 0L;

    private boolean running = true;

    private boolean active = true;

    boolean takeStats;

    private ProducerRate backOffSleeper;

    public StressorStats stats;

    private StressorParameter parameters;

    private CacheWrapper cacheWrapper;


        /* ******************* */
        /* *** CONSTRUCTOR *** */
        /* ******************* */

    public Consumer(CacheWrapper cacheWrapper, int threadIndex, StressorParameter parameters) {
        super("Stressor-" + threadIndex);
        this.parameters = parameters;
        stats = new StressorStats();
        this.cacheWrapper = cacheWrapper;
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

        /* *************** */
        /* *** METHODS *** */
        /* *************** */

    private void copyTimeStampInformation(Transaction oldTx, Transaction newTx){
        newTx.setEnqueueTimestamp(oldTx.getEnqueueTimestamp());
        newTx.setDequeueTimestamp(oldTx.getDequeueTimestamp());
        newTx.setStartTimestamp(oldTx.getStartTimestamp());
    }

    private Transaction regenerate(Transaction oldTransaction, int threadIndex, boolean lastSuccessful) {

        if (!lastSuccessful && !parameters.isRetrySameXact()) {
            this.backoffIfNecessary();
            Transaction newTransaction = generateTransaction(new RequestType(System.nanoTime(), oldTransaction.getType()), threadIndex);
            copyTimeStampInformation(oldTransaction, newTransaction);
            log.info("Thread " + threadIndex + ": regenerating a transaction of type " + oldTransaction.getType() +
                    " into a transaction of type " + newTransaction.getType());
            return newTransaction;
        }
        //If this is the first time xact runs or exact retry on abort is enabled...
        return oldTransaction;
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

    @Override
    public void run() {
        // TODO refactoring!!!

        Transaction tx;

        try {
            parameters.getStartPoint().await();
            log.info("Starting thread: " + getName());
        } catch (InterruptedException e) {
            log.warn("Interrupted while waiting for starting in " + getName());
        }

        long dequeueTimestamp = -1;



        boolean successful = true;

        while (assertRunning()) {
                /* 1- Extracting && generating request from queue */
            tx = null;
            dequeueTimestamp = -1;

            if(workloadGenerator.getSystemType().compareTo(AbstractWorkloadGenerator.SystemType.MULE) == 0){ // no queue
                tx = choiceTransaction(cacheWrapper.isPassiveReplication(), cacheWrapper.isTheMaster(), threadIndex);
                log.info("Closed system: starting a brand new transaction of type " + tx.getType());

            } else { // OPEN or CLOSED ==> queue!

                try {
                    RequestType request = queue.take();
                    dequeueTimestamp = System.nanoTime();

                    tx = generateTransaction(request, threadIndex);
                    tx.setEnqueueTimestamp(request.enqueueTimestamp);
                    tx.setDequeueTimestamp(dequeueTimestamp);

//                      COMMENTATO POICHé NON DOVREI MAI ENTRARE QUI
//                        if PassiveReplication so skip whether:
//                        a) master node && readOnly transaction
//                        b) slave node && write transaction
//                        boolean masterAndReadOnlyTx = cacheWrapper.isTheMaster() && tx.isReadOnly();
//                        boolean slaveAndWriteTx = (!cacheWrapper.isTheMaster() && !tx.isReadOnly());
//
//                        if (cacheWrapper.isPassiveReplication() && (masterAndReadOnlyTx || slaveAndWriteTx)) {
//                            continue;
//                        }


                        /* updating queue stats */
                    stats._handleQueueTx(tx);

                } catch (InterruptedException ir) {
                    log.error("»»»»»»»THREAD INTERRUPTED WHILE TRYING GETTING AN OBJECT FROM THE QUEUE«««««««");
                }

            }

                /* 2- Executing the transaction */

            successful = processTransaction(tx); /* it executes the retryOnAbort (if enabled) */
            if (workloadGenerator.getSystemType().compareTo(AbstractWorkloadGenerator.SystemType.CLOSED) == 0) {  //Closed system   --> no queueing time
                tx.setDequeueTimestamp( tx.getStartTimestamp() ); // No queuing time
            }
            stats._handleEndTx(tx,successful);

            // notify the producer
            tx.notifyProducer();


            blockIfInactive();
        }
    }


    private void backoffIfNecessary() {
        if (backOffTime != 0) {
            stats.inc(StressorStats.NUM_BACK_OFFS);
            long backedOff = backOffSleeper.sleep();
            log.info("Thread " + this.threadIndex + " backed off for " + backedOff + " msec");
            stats.put(StressorStats.BACKED_OFF_TIME, backedOff);
        }
    }

    private boolean startNewTransaction(boolean lastXactSuccessul) {
        return !retryOnAbort || lastXactSuccessul;
    }

        /*
        public long totalDuration() {
            return readDuration + writeDuration;
        }
        */

    private synchronized boolean assertRunning() {
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

    private synchronized void blockIfInactive() {
        while (!active) {
            try {
                wait();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
