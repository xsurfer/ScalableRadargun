package org.radargun.stressors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.CacheWrapper;
import org.radargun.Transaction;
import org.radargun.portings.tpcc.ElementNotFoundException;
import org.radargun.portings.tpcc.transaction.NewOrderTransaction;
import org.radargun.portings.tpcc.transaction.PaymentTransaction;
import org.radargun.producer.GroupProducerRateFactory;
import org.radargun.producer.ProducerRate;
import org.radargun.stages.AbstractBenchmarkStage;
import org.radargun.stressors.commons.StressorStats;
import org.radargun.utils.StatSampler;
import org.radargun.workloadGenerator.AbstractWorkloadGenerator;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by: Fabio Perfetti
 * E-mail: perfabio87@gmail.com
 * Date: 4/1/13
 */
public abstract class AbstractBenchmarkStressor extends AbstractCacheWrapperStressor implements Observer {

    /* **************** */
    /* *** COSTANTS *** */
    /* **************** */

    //in milliseconds, each producer sleeps for this time in average
    protected static final int AVERAGE_PRODUCER_SLEEP_TIME = 10;



    /* ****************** */
    /* *** ATTRIBUTES *** */
    /* ****************** */

    private static Log log = LogFactory.getLog(AbstractCacheWrapperStressor.class);

    protected AtomicLong countJobs;

    protected final AtomicBoolean running = new AtomicBoolean(true);

    /**
     * last arrival rate of the transactions to the system (transactions per second)
     */
    protected int lastArrivalRate = 0;

    /**
     * specify the interval period (in milliseconds) of the memory and cpu usage is collected
     */
    protected long statsSamplingInterval = 0;

    protected StatSampler statSampler;

    private final Timer finishBenchmarkTimer = new Timer("Finish-Benchmark-Timer");

    protected long startTime;

    protected long endTime;

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

    protected BlockingQueue<RequestType> queue;

    protected final List<Producer> producers = Collections.synchronizedList(new ArrayList<Producer>());

    protected final List<Consumer> consumers = Collections.synchronizedList(new LinkedList<Consumer>());

    protected AbstractWorkloadGenerator workloadGenerator;

    protected AbstractBenchmarkStage benchmarkStage;


    /* ******************* */
    /* *** CONSTRUCTOR *** */
    /* ******************* */

    public AbstractBenchmarkStressor(AbstractWorkloadGenerator loadGenerator) {
        this.workloadGenerator = loadGenerator;
        log.trace("Registring this TpccStressor to the WorkLoadGenerator (Observable)");
        this.workloadGenerator.addObserver(this);
    }



    /* ******************* */
    /* *** TO OVERRIDE *** */
    /* ******************* */

    protected abstract void validateTransactionsWeight();

    protected abstract void initialization();

    protected abstract RequestType nextTransaction();

    protected abstract Transaction generateTransaction(RequestType type, int threadIndex, StressorStats stats);

    public abstract Transaction choiceTransaction(boolean isPassiveReplication, boolean isTheMaster, int threadId);

    protected abstract Map<String, String> processResults(List<Consumer> stressors);

    protected abstract double getWriteWeight();

    protected abstract double getReadWeight();


    /* ****************** */
    /* ***** METHODS **** */
    /* ****************** */

    public final Map<String, String> stress(CacheWrapper wrapper) {
        if (wrapper == null) {
            throw new IllegalStateException("Null wrapper not allowed");
        }
        this.cacheWrapper = wrapper;

        /* Registering to the observable */
        wrapper.addObserver(this);

        validateTransactionsWeight();
        initialization();

        if (workloadGenerator.getSystemType().compareTo(AbstractWorkloadGenerator.SystemType.OPEN) == 0) {
            queue = new LinkedBlockingQueue<RequestType>();
            countJobs = new AtomicLong(0L);

            log.info("Open System");
            log.info("MaxArrivalRate: " + workloadGenerator.getMaxArrivalRate());
            log.info("Granularity: " + workloadGenerator.getGranularity());
            log.info("InitTime: " + workloadGenerator.getInitTime());
        } else {
            log.info("Closed System");
        }


        if (statsSamplingInterval > 0) {
            statSampler = new StatSampler(statsSamplingInterval);
        }

        startTime = System.currentTimeMillis();
        log.info("Executing: " + this.toString());

        if (perThreadSimulTime > 0) {
            finishBenchmarkTimer.schedule(new TimerTask() {
                @Override
                public void run() {
                    finishBenchmark();
                }
            }, perThreadSimulTime * 1000);
        } else if (perThreadSimulTime <= 0) {
            log.warn("Slave arrived too late, it will works on the next stage");
            return null;
        }


        if (workloadGenerator.getSystemType()) {
            log.trace("Workload generator started");
            workloadGenerator.start();
        }

        if (statSampler != null) {
            log.trace("Sampler started");
            statSampler.start();
        }
        try {
            executeOperations();
        } catch (Exception e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }


        if (statSampler != null) {
            statSampler.cancel();
        }
        return processResults(consumers);
    }

    protected List<Consumer> executeOperations() {

        startPoint = new CountDownLatch(1);
        for (int threadIndex = 0; threadIndex < numOfThreads; threadIndex++) {
            Consumer consumer = new Consumer(threadIndex);
            consumers.add(consumer);
            consumer.start();
        }
        log.info("Cache wrapper info is: " + cacheWrapper.getInfo());
        startPoint.countDown();
        blockWhileRunning();
        for (Consumer consumer : consumers) {
            try {
                consumer.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        endTime = System.currentTimeMillis();
        return consumers;
    }

    private synchronized void blockWhileRunning() {
        while (running.get()) {
            try {
                wait();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private final synchronized void finishBenchmark() {
        if (running.compareAndSet(true, false)) {
            stopStressor();
            if (workloadGenerator.getSystemType()) {
                log.trace("Stopping producers");
                stopProducers();
            }
            log.trace("Stopping workload generator");
            workloadGenerator.stop();
            log.trace("Waking up waiting thread");
            notifyAll();
        }
    }

    private void stopStressor() {
        synchronized (consumers) {
            for (Consumer stressor : consumers) {
                stressor.finish();
            }
        }
    }

    private void stopProducers() {
        synchronized (producers) {
            for (Producer producer : producers) {
                producer.interrupt();
            }
        }
    }

    private void startProducers() {
        synchronized (producers) {
            for (Producer producer : producers) {
                producer.start();
            }
        }
    }

    private void updateProducer() {
        synchronized (running) {
            if (running.get()) {
                log.info("Stopping old producer");
                stopProducers();
                createProducers(workloadGenerator.getArrivalRate());
                log.info("Starting " + producers.size() + " producers");
                startProducers();
            }
        }
    }

    /**
     * This method is executed each time that the workload wake up
     *
     * @param o
     * @param arg
     */
    @Override
    public final void update(Observable o, Object arg) {
        Integer cmd = (Integer) arg;
        switch (cmd) {
            case CacheWrapper.VIEW_CHANGED:
                log.info("VIEW has changed: #slaves = " + cacheWrapper.getNumMembers());
                updateProducer();
                break;
            case AbstractWorkloadGenerator.ARRIVAL_RATE_CHANGED:
                log.info("Arrival rate changed");
                if (workloadGenerator.getArrivalRate() != this.lastArrivalRate) {
                    this.lastArrivalRate = workloadGenerator.getArrivalRate();
                    updateProducer();
                }
                break;
            default:
                log.warn("Unrecognized argument");
                break;
        }
    }

    /**
     * Class in charge of create or update the Producer in base of
     *
     * @param arrivalRate
     */
    private void createProducers(int arrivalRate) {

        if (!this.workloadGenerator.getSystemType()) {
            return;
        } // Closed System

        log.info("Creating/Updating producers");

        ProducerRate[] producerRates;
        if (cacheWrapper.isPassiveReplication()) {
            if (cacheWrapper.isTheMaster()) {
                log.info("Creating producers groups for the master. Write transaction percentage is " + getWriteWeight());
                producerRates = new GroupProducerRateFactory(workloadGenerator.getRateDistribution(),
                        getWriteWeight(),
                        1,
                        nodeIndex,
                        AVERAGE_PRODUCER_SLEEP_TIME).create();
            } else {
                log.info("Creating producers groups for the slave. Read-only transaction percentage is " + getReadWeight());
                producerRates = new GroupProducerRateFactory(workloadGenerator.getRateDistribution(),
                        getReadWeight(),
                        cacheWrapper.getNumMembers() - 1,
                        nodeIndex == 0 ? nodeIndex : nodeIndex - 1,
                        AVERAGE_PRODUCER_SLEEP_TIME).create();
            }
        } else {
            log.info("Creating producers groups");
            producerRates = new GroupProducerRateFactory(workloadGenerator.getRateDistribution(), arrivalRate, cacheWrapper.getNumMembers(), nodeIndex,
                    AVERAGE_PRODUCER_SLEEP_TIME).create();
        }
        //producers = new Producer[producerRates.length];

        synchronized (producers) {
            producers.clear();
            for (int i = 0; i < producerRates.length; ++i) {
                producers.add(i, new Producer(producerRates[i], i));
                //producers[i] = new Producer(producerRates[i], i);
            }
        }
    }


    /* ******************** */
    /* *** GETTER/SETTER ** */
    /* ******************** */


   /* ************************** */
   /* ***** PRODUCER CLASS ***** */
   /* ************************** */

    protected class Producer extends Thread {
        private final ProducerRate rate;
        private boolean running = false;

        public Producer(ProducerRate rate, int id) {
            super("Producer-" + id);
            setDaemon(true);
            this.rate = rate;
        }

        public void run() {
            if (log.isDebugEnabled()) {
                log.debug("Starting " + getName() + " with rate of " + rate.getLambda());
            }
            while (assertRunning()) {

                queue.offer(nextTransaction());
                countJobs.incrementAndGet();
                rate.sleep();
            }
        }

        private synchronized boolean assertRunning() {
            return running;
        }

        @Override
        public synchronized void start() {
            if (running) return;
            running = true;
            super.start();
        }

        @Override
        public synchronized void interrupt() {
            if (!running) return;
            running = false;
            super.interrupt();
        }
    }




   /* ************************** */
   /* ***** CONSUMER CLASS ***** */
   /* ************************** */

    protected class Consumer extends Thread {
        private int threadIndex;
        //private double arrivalRate;

        private boolean running = true;

        private boolean active = true;

        private ProducerRate backOffSleeper;

        private StressorStats stats;

        public Consumer(int threadIndex) {
            super("Stressor-" + threadIndex);

            if (backOffTime > 0)
                try {
                    this.backOffSleeper =
                            ProducerRate.createInstance(workloadGenerator.getRateDistribution(), Math.pow((double) backOffTime, -1D));
                } catch (ProducerRate.ProducerRateException e) {
                    throw new RuntimeException(e);
                }
        }

        private Transaction regenerate(Transaction oldTransaction, int threadIndex, boolean lastSuccessful) {

            if (!lastSuccessful && !retrySameXact) {
                this.backoffIfNecessary();
                Transaction newTransaction = generateTransaction(oldTransaction.getType(), threadIndex);
                log.info("Thread " + threadIndex + ": regenerating a transaction of type " + oldTransaction.getType() +
                        " into a transaction of type " + newTransaction.getType());
                return newTransaction;
            }
            //If this is the first time xact runs or exact retry on abort is enabled...
            return oldTransaction;
        }

        @Override
        public void run() {

            try {
                startPoint.await();
                log.info("Starting thread: " + getName());
            } catch (InterruptedException e) {
                log.warn("Interrupted while waiting for starting in " + getName());
            }

            long end, endInQueueTime, commit_start = 0L;
            boolean isReadOnly, successful, measureCommitTime = false, takeStats;
            Transaction transaction;


            while (assertRunning()) {
                transaction = null;
                long start = -1;
                if (workloadGenerator.getSystemType()) {  //Open system
                    try {
                        RequestType request = queue.take();
                        transaction = generateTransaction(request, threadIndex, stats);

                        // if PassiveReplication so skip whether:
                        // a) master node && readOnly transaction
                        // b) slave node && write transaction
                        boolean masterAndReadOnlyTx = cacheWrapper.isTheMaster() && transaction.isReadOnly();
                        boolean slaveAndWriteTx = (!cacheWrapper.isTheMaster() && !transaction.isReadOnly());

                        if (cacheWrapper.isPassiveReplication() && (masterAndReadOnlyTx || slaveAndWriteTx)) {
                            continue;
                        }
                        start = request.timestamp;
                    } catch (InterruptedException ir) {
                        log.error("»»»»»»»THREAD INTERRUPTED WHILE TRYING GETTING AN OBJECT FROM THE QUEUE«««««««");
                    }
                } else {
                    transaction = choiceTransaction(cacheWrapper.isPassiveReplication(), cacheWrapper.isTheMaster(), threadIndex);
                    log.info("Closed system: starting a brand new transaction of type " + transaction.getType());
                }
                isReadOnly = transaction.isReadOnly();

                long startService = System.nanoTime();
                boolean elementNotFoundExceptionThrown = false;
                successful = true; //so that backOffIfNecessary returns false the first time we run an xact
                do {
                    transaction = regenerate(transaction, threadIndex, successful);
                    successful = true;
                    cacheWrapper.startTransaction();
                    try {
                        transaction.executeTransaction(cacheWrapper);
                        log.info("Thread " + threadIndex + " successfully completed locally a transaction of type " +
                                transaction.getType() + " btw, successful is " + successful);
                    } catch (Throwable e) {
                        successful = false;
                        if (log.isDebugEnabled()) {
                            log.debug("Exception while executing transaction.", e);
                        } else {
                            log.warn("Exception while executing transaction of type: " + transaction.getType() + " " + e.getMessage());
                        }
                        if (e instanceof ElementNotFoundException) {
                            this.appFailures++;
                            elementNotFoundExceptionThrown = true;
                        } else if (cacheWrapper.isTimeoutException(e))
                            localTimeout++;

                    }

                    //here we try to finalize the transaction
                    //if any read/write has failed we abort

                    try {

                        /* In our tests we are interested in the commit time spent for write txs */
                        if (successful && !isReadOnly) {
                            commit_start = System.nanoTime();
                            measureCommitTime = true;
                        }
                        cacheWrapper.endTransaction(successful, threadIndex);
                        if (successful) {
                            log.info("Thread " + threadIndex + " successfully completed remotely a transaction of type " +
                                    transaction.getType() + " Btw, successful is " + successful);
                        } else {
                            nrFailures++;
                            if (!isReadOnly) {
                                nrWrFailures++;
                                if (transaction instanceof NewOrderTransaction) {
                                    nrNewOrderFailures++;
                                } else if (transaction instanceof PaymentTransaction) {
                                    nrPaymentFailures++;
                                }
                            } else {
                                nrRdFailures++;
                            }

                        }
                    } catch (Throwable rb) {
                        nrFailures++;
                        //if (cacheWrapper.isTimeoutException(rb))
                        remoteTimeout++;
                        successful = false;
                        if (!isReadOnly) {
                            nrWrFailures++;
                            nrWrFailuresOnCommit++;
                            if (transaction instanceof NewOrderTransaction) {
                                nrNewOrderFailures++;
                            } else if (transaction instanceof PaymentTransaction) {
                                nrPaymentFailures++;
                            }
                        } else {
                            nrRdFailures++;
                        }
                        if (log.isDebugEnabled()) {
                            log.debug("Error while committing", rb);
                        } else {
                            log.warn("Error while committing: " + rb.getMessage());
                        }
                    }
                    log.info("Successful = " + successful + " elementNotFoundException " + elementNotFoundExceptionThrown);
                }
                //If we experience an elementNotFoundException we do not want to restart the very same xact!!
                //If a xact is not progressing at the end of the test, we kill it. Some stats will be slightly affected by this
                while (assertRunning() && retryOnAbort && !successful && !elementNotFoundExceptionThrown);

                end = System.nanoTime();

                if (!workloadGenerator.getSystemType()) {  //Closed system   --> no queueing time
                    start = startService;
                }

                if (!isReadOnly) {
                    writeDuration += end - start;
                    writeServiceTime += end - startService;
                    if (transaction instanceof NewOrderTransaction) {
                        newOrderDuration += end - start;
                        newOrderServiceTime += end - startService;
                    } else if (transaction instanceof PaymentTransaction) {
                        paymentDuration += end - start;
                        paymentServiceTime += end - startService;
                    }
                    if (successful) {
                        successful_writeDuration += end - startService;
                        writes++;
                        if (transaction instanceof PaymentTransaction) {
                            payment++;
                        } else if (transaction instanceof NewOrderTransaction) {
                            newOrder++;
                        }
                    }
                } else {
                    readDuration += end - start;
                    readServiceTime += end - startService;
                    if (successful) {
                        reads++;
                        successful_readDuration += end - startService;
                    }
                }

                if (measureCommitTime) {    //We sample just the last commit time, i.e., the successful one
                    if (successful) {
                        this.successful_commitWriteDuration += end - commit_start;
                    } else {
                        this.aborted_commitWriteDuration += end - commit_start;
                    }
                    this.commitWriteDuration += end - commit_start;
                }

                blockIfInactive();
            }
        }


        private void backoffIfNecessary() {
            if (backOffTime != 0) {
                this.numBackOffs++;
                long backedOff = backOffSleeper.sleep();
                log.info("Thread " + this.threadIndex + " backed off for " + backedOff + " msec");
                this.backedOffTime += backedOff;
            }
        }

        private boolean startNewTransaction(boolean lastXactSuccessul) {
            return !retryOnAbort || lastXactSuccessul;
        }

        public long totalDuration() {
            return readDuration + writeDuration;
        }

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

    private void saveSamples() {
        if (statSampler == null) {
            return;
        }
        log.info("Saving samples in the file sample-" + nodeIndex);
        File f = new File("sample-" + nodeIndex);
        try {
            BufferedWriter bw = new BufferedWriter(new FileWriter(f));
            List<Long> mem = statSampler.getMemoryUsageHistory();
            List<Double> cpu = statSampler.getCpuUsageHistory();

            int size = Math.min(mem.size(), cpu.size());
            bw.write("#Time (milliseconds)\tCPU(%)\tMemory(bytes)");
            bw.newLine();
            for (int i = 0; i < size; ++i) {
                bw.write((i * statsSamplingInterval) + "\t" + cpu.get(i) + "\t" + mem.get(i));
                bw.newLine();
            }
            bw.flush();
            bw.close();
        } catch (IOException e) {
            log.warn("IOException caught while saving sampling: " + e.getMessage());
        }
    }



    /* ************************ */
    /* *** RequestType CLASS ** */
    /* ************************ */

    protected class RequestType {

        public long timestamp;
        public int transactionType;

        public RequestType(long timestamp, int transactionType) {
            this.timestamp = timestamp;
            this.transactionType = transactionType;
        }

    }
}
