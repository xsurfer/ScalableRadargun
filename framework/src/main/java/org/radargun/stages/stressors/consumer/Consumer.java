package org.radargun.stages.stressors.consumer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.*;
import org.radargun.stages.AbstractBenchmarkStage;
import org.radargun.stages.stressors.AbstractBenchmarkStressor;
import org.radargun.stages.stressors.Parameters;
import org.radargun.stages.stressors.commons.StressorStats;
import org.radargun.stages.stressors.exceptions.ApplicationException;
import org.radargun.stages.stressors.producer.Producer;
import org.radargun.stages.stressors.producer.ProducerRate;
import org.radargun.stages.stressors.producer.RequestType;
import org.radargun.stages.stressors.systems.*;
import org.radargun.stages.stressors.systems.System;
import org.radargun.stages.synthetic.XACT_RETRY;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by: Fabio Perfetti
 * E-mail: perfabio87@gmail.com
 * Date: 4/24/13
 */
public class Consumer implements IConsumer {

    private static Log log = LogFactory.getLog(Consumer.class);


    protected CacheWrapper cacheWrapper;

    protected final int threadIndex;

    protected System system;

    protected AbstractBenchmarkStage stage;

    protected AbstractBenchmarkStressor stressor;

    protected final Parameters parameters;

    public StressorStats stats;

    protected ProducerRate backOffSleeper;

    protected volatile boolean running = true;

    protected AtomicBoolean active = new AtomicBoolean(true);

    protected final TransactionFactory factory;

    /* ******************* */
    /* *** CONSTRUCTOR *** */
    /* ******************* */

    public Consumer(CacheWrapper cacheWrapper,
                    int threadIndex,
                    System system,
                    AbstractBenchmarkStage stage,
                    AbstractBenchmarkStressor stressor,
                    Parameters parameters,
                    TransactionFactory factory) {


        this.threadIndex = threadIndex;
        this.cacheWrapper = cacheWrapper;
        this.system = system;
        this.stage = stage;
        this.stressor = stressor;
        this.parameters = parameters;
        this.factory = factory;

        stats = new StressorStats();

        if (parameters.getBackOffTime() > 0)
            try {
                // TODO renderlo customizable
                this.backOffSleeper =
                        ProducerRate.createInstance(RateDistribution.EXPONENTIAL,
                                Math.pow((double) parameters.getBackOffTime(), -1D)
                        );
            } catch (ProducerRate.ProducerRateException e) {
                throw new RuntimeException(e);
            }
    }

    /* ******************* */
    /* *** TO OVERRIDE *** */
    /* ******************* */

    public void consume(OpenSystem system){
        ITransaction tx;
        long dequeueTimestamp = -1;
        boolean successful = true;

        while (assertRunning()) {
            /* 1- Extracting && generating request from queue */
            tx = null;
            dequeueTimestamp = -1;

            RequestType request = stressor.takeFromQueue();
            if(request.getTransactionType() != 9999){
                dequeueTimestamp = java.lang.System.nanoTime();

                tx = factory.generateTransaction(request);
                //tx.setEnqueueTimestamp(request.enqueueTimestamp);
                GeneratedTransactionDecorator generatedTx = new GeneratedTransactionDecorator(tx, request, dequeueTimestamp);


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
                stats._handleQueueTx(generatedTx);

            /* 2- Executing the transaction */
                successful = processTransaction(generatedTx); /* it executes the retryOnAbort (if enabled) */
                stats._handleEndTx(generatedTx, successful);

                blockIfInactive();
                log.trace("Consumer = { active: " + active + " ; running: " + running + "}");
            }
        }
        log.info("Out of the while");
    }

    public void consume(ClosedSystem system) {
        log.info("Consuming in closed system scenario");
        ITransaction tx;
        long dequeueTimestamp = -1;
        boolean successful = true;

        while (assertRunning()) {
            /* 1- Extracting && generating request from queue */
            tx = null;
            dequeueTimestamp = -1;

            RequestType request = stressor.takeFromQueue();
            if(request.getTransactionType() != 9999){

                dequeueTimestamp = java.lang.System.nanoTime();

                tx = factory.generateTransaction(request);
                GeneratedTransactionDecorator generatedTx = new GeneratedTransactionDecorator(tx, request, dequeueTimestamp);

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
                stats._handleQueueTx(generatedTx);

            /* 2- Executing the transaction */
                successful = processTransaction(generatedTx); /* it executes the retryOnAbort (if enabled) */
                stats._handleEndTx(generatedTx, successful);

                // notify the producer
                notifyProducer(generatedTx.getRequestType().getProducer());
                //tx.notifyProducer();

                blockIfInactive();
                log.trace("Consumer = { active: " + active + " ; running: " + running + "}");
            }
        }
        log.info("Out of the while");
    }

    public void consume(MuleSystem system) {
        log.info("Consuming in mule system scenario");
        ProducerRate rate;
        try {
            rate = ProducerRate.createInstance(system.getRateDistribution(), Math.pow((double) system.getThinkTime(), -1D) );

            log.info("{ Distribution = " + system.getRateDistribution().toString() + ", ThinkTime = " + system.getThinkTime() );
        } catch (ProducerRate.ProducerRateException e) {
            throw new RuntimeException(e);
        }

        ITransaction tx;

        long dequeueTimestamp = -1;
        boolean successful = true;

        while (assertRunning()) {
            /* 1- Extracting && generating request from queue */
            tx = null;
            dequeueTimestamp = -1;

            //log.info("Mule system: starting a brand new transaction of type " + tx.getType());
            tx = factory.choiceTransaction();
            CreatedTransactionDecorator createdTx = new CreatedTransactionDecorator(tx); // No queuing
            /* 2- Executing the transaction */
            successful = processTransaction(createdTx); /* it executes the retryOnAbort (if enabled) */

            stats._handleEndTx(createdTx, successful);
            log.trace("Asleep: " + java.lang.System.currentTimeMillis() );
            rate.sleep();
            log.trace("Awake: " + java.lang.System.currentTimeMillis() );


            blockIfInactive();
        }
    }



    /* *************** */
    /* *** METHODS *** */
    /* *************** */

    @Override
    public final void run() {
        // TODO refactoring!!!

        synchronize();
        system.consume(this);
    }


    protected void synchronize(){
        try {
            parameters.getStartPoint().await();
            log.info("Starting thread: " + Thread.currentThread().getName());
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void notifyProducer(Producer producer){
        if( producer == null ) { throw new IllegalStateException("No producer to notify (open system?)"); }
        producer.doNotify();
    }

    protected boolean processTransaction(TransactionDecorator tx) {
        // entrambi le fasi devono essere fatte!!!

        boolean successful = true;
        boolean isSafeToRetry = true;
        boolean localAbort = false;
        boolean remoteAbort = false;

        stats._handleStartsTx(tx);
        //TODO per diego, gestisciti CCTP i retry
        do {
            tx = regenerate(tx, threadIndex, successful);
            successful = true;
            cacheWrapper.startTransaction();
            try {
                tx.executeTransaction(cacheWrapper);
                stats._handleSuccessLocalTx(tx);
                log.trace("Thread " + threadIndex + " successfully completed locally a transaction of type " +
                        tx.getType() + " btw, successful is " + successful);

            } catch (Throwable e) {
                localAbort= true;
                successful = false;
//                if (log.isDebugEnabled()) {
//                    log.debug("Exception while executing transaction.", e);
//                } else {
                log.trace("Exception while executing transaction locally (type: " + tx.getType() + ", readOnly: " + tx.isReadOnly() + "). Message: " + e.getMessage());
//                }

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
                    log.trace("Thread " + threadIndex + " successfully completed remotely a transaction of type " +
                            tx.getType() + " Btw, successful is " + successful);
                }
            } catch (Throwable e) {
                // error during rollback o commit
                if( successful ){ // errore nel commit
                    remoteAbort=true;
                    successful = false;
                    stats._handleAbortRemoteTx(tx, e);
                } else {
                    log.trace("What's going on?");
                }

                if (log.isDebugEnabled()) {
                    log.debug("Error while committing", e);
                } else {
                    log.trace("Error while committing: " + e.getMessage());
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


    protected TransactionDecorator regenerate(TransactionDecorator transaction, int threadIndex, boolean lastSuccessful) {

        if (!lastSuccessful && parameters.getRetryOnAbort().equals(XACT_RETRY.RETRY_SAME_CLASS) ) {
            backoffIfNecessary();
            ITransaction newTransaction = factory.generateTransaction(new RequestType(java.lang.System.nanoTime(), transaction.getType()));

            transaction.regenerate(newTransaction);
            //copyTimeStampInformation(transaction, newTransaction);
            log.trace("Thread " + threadIndex + ": regenerating a transaction (readOnly: " + transaction.isReadOnly() + ") of type " + transaction.getType() +
                    " into a transaction of type " + newTransaction.getType());
            return transaction;
        }
        // If this is the first time xact runs or exact retry on abort is enabled...
        return transaction;
    }


    protected void backoffIfNecessary() {
        if (parameters.getBackOffTime() != 0) {
            stats.inc(StressorStats.NUM_BACK_OFFS);
            long backedOff = backOffSleeper.sleep();
            log.trace("Thread " + this.threadIndex + " backed off for " + backedOff + " msec");
            stats.put(StressorStats.BACKED_OFF_TIME, backedOff);
        }
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


    public final synchronized void disable() {
        active.set(false);
    }


    public final synchronized void enable() {
        active.set(true);
        notifyAll();
    }


    public final synchronized void finish() {
        active.set(true);
        running = false;
        notifyAll();
        log.info("Consumer stopped");
    }


    public final synchronized boolean isEnabled() {
        return active.get();
    }


    protected synchronized void blockIfInactive() {
        while (!active.get()) {
            try {
                wait();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public String toString(){
        return new String("I'm a consumer, index: " + threadIndex);
    }

    public int getThreadIndex(){
        return threadIndex;
    }


}
