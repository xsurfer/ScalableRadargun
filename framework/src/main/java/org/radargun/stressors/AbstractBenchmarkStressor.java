package org.radargun.stressors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.CacheWrapper;
import org.radargun.Transaction;
import org.radargun.producer.GroupProducerRateFactory;
import org.radargun.producer.ProducerRate;
import org.radargun.stages.AbstractBenchmarkStage;
import org.radargun.stressors.commons.StressorStats;
import org.radargun.utils.StatSampler;
import org.radargun.utils.Utils;
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
 * @author Fabio Perfetti
 * E-mail: perfabio87@gmail.com
 * Date: 4/1/13
 */
public abstract class AbstractBenchmarkStressor<T extends AbstractBenchmarkStressor.Consumer, S extends StressorStats> extends AbstractCacheWrapperStressor implements Observer {

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

    protected final Timer finishBenchmarkTimer = new Timer("Finish-Benchmark-Timer");

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

    protected final List<T> consumers = Collections.synchronizedList(new LinkedList<T>());

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

    protected abstract void initialization();

    protected abstract RequestType nextTransaction();

    protected abstract Transaction generateTransaction(RequestType type, int threadIndex);

    protected abstract Transaction choiceTransaction(boolean isPassiveReplication, boolean isTheMaster, int threadId);

    //protected abstract Map<String, String> processResults(List<T> stressors);

    protected abstract double getWriteWeight();

    protected abstract double getReadWeight();

    protected abstract void validateTransactionsWeight();

    protected abstract T createConsumer(int threadIndex);

    protected abstract void extractExtraStats(S totalStats, S singleStats);

    protected abstract void fillMapWithExtraStats(S totalStats, Map<String, String> results);



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


        if (workloadGenerator.getSystemType().compareTo(AbstractWorkloadGenerator.SystemType.OPEN) == 0) {
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

    protected void executeOperations() {

        startPoint = new CountDownLatch(1);
        for (int threadIndex = 0; threadIndex < numOfThreads; threadIndex++) {
            T consumer = createConsumer(threadIndex);
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
        //return consumers;
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

    protected final synchronized void finishBenchmark() {
        if (running.compareAndSet(true, false)) {
            stopStressor();
            if (workloadGenerator.getSystemType().compareTo(AbstractWorkloadGenerator.SystemType.OPEN) == 0) {
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


    public abstract S createStatsContainer();

    protected Map<String, String> processResults(List<T> consumers) {

        long duration = 0;
        S totalStats = createStatsContainer();

        /* 1) Extracting per consumer stats */
        for (T consumer : consumers) {

            S singleStats = (S) consumer.stats;

            //readsDurations += stressor.readDuration; //in nanosec
            //writesDurations += stressor.writeDuration; //in nanosec

            totalStats.inc(StressorStats.WRITE_DURATION, singleStats.get(StressorStats.SUCCESSFUL_WRITE_DURATION)); // in nanosec
            totalStats.inc(StressorStats.READ_DURATION, singleStats.get(StressorStats.SUCCESSFUL_READ_DURATION)); // in nanosec
            totalStats.inc(StressorStats.SUCCESSFUL_COMMIT_WRITE_DURATION, singleStats.get(StressorStats.SUCCESSFUL_COMMIT_WRITE_DURATION)); // in nanosec
            totalStats.inc(StressorStats.ABORTED_COMMIT_WRITE_DURATION, singleStats.get(StressorStats.ABORTED_COMMIT_WRITE_DURATION)); // in nanosec
            totalStats.inc(StressorStats.COMMIT_WRITE_DURATION, singleStats.get(StressorStats.COMMIT_WRITE_DURATION)); // in nanosec
            totalStats.inc(StressorStats.WRITE_SERVICE_TIME, singleStats.get(StressorStats.WRITE_SERVICE_TIME));
            totalStats.inc(StressorStats.READ_SERVICE_TIME, singleStats.get(StressorStats.READ_SERVICE_TIME));
            totalStats.inc(StressorStats.READS, singleStats.get(StressorStats.READS));
            totalStats.inc(StressorStats.WRITES, singleStats.get(StressorStats.WRITES));
            totalStats.inc(StressorStats.NR_FAILURES, singleStats.get(StressorStats.NR_FAILURES));
            totalStats.inc(StressorStats.NR_RD_FAILURES, singleStats.get(StressorStats.NR_RD_FAILURES));
            totalStats.inc(StressorStats.NR_WR_FAILURES, singleStats.get(StressorStats.NR_WR_FAILURES));
            totalStats.inc(StressorStats.NR_WR_FAILURES_ON_COMMIT, singleStats.get(StressorStats.NR_WR_FAILURES_ON_COMMIT));
            totalStats.inc(StressorStats.APP_FAILURES, singleStats.get(StressorStats.APP_FAILURES));
            totalStats.inc(StressorStats.WRITE_IN_QUEUE_TIME, singleStats.get(StressorStats.WRITE_IN_QUEUE_TIME));
            totalStats.inc(StressorStats.READ_IN_QUEUE_TIME, singleStats.get(StressorStats.READ_IN_QUEUE_TIME));
            totalStats.inc(StressorStats.NUM_WRITE_DEQUEUED, singleStats.get(StressorStats.NUM_WRITE_DEQUEUED));
            totalStats.inc(StressorStats.NUM_READ_DEQUEUED, singleStats.get(StressorStats.NUM_READ_DEQUEUED));
            totalStats.inc(StressorStats.LOCAL_TIMEOUT, singleStats.get(StressorStats.LOCAL_TIMEOUT));
            totalStats.inc(StressorStats.REMOTE_TIMEOUT, singleStats.get(StressorStats.REMOTE_TIMEOUT));
            totalStats.inc(StressorStats.NUM_BACK_OFFS, singleStats.get(StressorStats.NUM_BACK_OFFS));
            totalStats.inc(StressorStats.BACKED_OFF_TIME, singleStats.get(StressorStats.BACKED_OFF_TIME));

            extractExtraStats(totalStats, singleStats);

        }

        //readsDurations = readsDurations / 1000; //nanosec to microsec
        //writesDurations = writesDurations / 1000; //nanosec to microsec

        /* 2) Converting from nanoseconds to milliseconds && filling the stats obj */
        totalStats.put(StressorStats.READ_DURATION, totalStats.get(StressorStats.READ_DURATION) / 1000); //nanosec to microsec
        totalStats.put(StressorStats.WRITE_DURATION, totalStats.get(StressorStats.WRITE_DURATION) / 1000); //nanosec to microsec
        totalStats.put(StressorStats.COMMIT_WRITE_DURATION, totalStats.get(StressorStats.COMMIT_WRITE_DURATION) / 1000); //nanosec to microsec
        totalStats.put(StressorStats.ABORTED_COMMIT_WRITE_DURATION, totalStats.get(StressorStats.ABORTED_COMMIT_WRITE_DURATION) / 1000); //nanosec to microsec
        totalStats.put(StressorStats.WRITE_SERVICE_TIME, totalStats.get(StressorStats.WRITE_SERVICE_TIME) / 1000); //nanosec to microsec
        totalStats.put(StressorStats.READ_SERVICE_TIME, totalStats.get(StressorStats.READ_SERVICE_TIME) / 1000); //nanosec to microsec
        totalStats.put(StressorStats.WRITE_IN_QUEUE_TIME, totalStats.get(StressorStats.WRITE_IN_QUEUE_TIME) / 1000); //nanosec to microsec
        totalStats.put(StressorStats.READ_IN_QUEUE_TIME, totalStats.get(StressorStats.READ_IN_QUEUE_TIME) / 1000); //nanosec to microsec

        totalStats.put(StressorStats.DURATION, endTime - startTime);


        /* 3) Filling the map */
        Map<String, String> results = new LinkedHashMap<String, String>();

        results.put("STOPPED", str(this.stoppedByJmx));
        results.put("DURATION (msec)", str(totalStats.get(StressorStats.DURATION) ));
        results.put("REQ_PER_SEC", str( totalStats.evalRequestPerSec() ));
        results.put("READS_PER_SEC", str( totalStats.evalRdPerSec() ));
        results.put("WRITES_PER_SEC", str( totalStats.evalWrtPerSec() ));
        results.put("READ_COUNT", str( totalStats.get( StressorStats.READS ) ) );
        results.put("WRITE_COUNT", str( totalStats.get( StressorStats.WRITES ) ) );
        results.put("FAILURES", str( totalStats.get(StressorStats.NR_FAILURES) ) );
        results.put("APPLICATION_FAILURES", str( totalStats.get(StressorStats.APP_FAILURES) ) );
        results.put("WRITE_FAILURES", str( totalStats.get(StressorStats.NR_WR_FAILURES) ) );
        results.put("READ_FAILURES", str( totalStats.get(StressorStats.NR_RD_FAILURES) ) );
        results.put("AVG_SUCCESSFUL_DURATION (usec)", str( totalStats.evalAvgSuccessfulDuration() ) );
        results.put("AVG_SUCCESSFUL_READ_DURATION (usec)", str( totalStats.evalAvgSuccessfulReadDuration() ) );
        results.put("AVG_SUCCESSFUL_WRITE_DURATION (usec)", str( totalStats.evalAvgSuccessfulWriteDuration() ) );
        results.put("AVG_SUCCESSFUL_COMMIT_WRITE_DURATION (usec)", str( totalStats.evalAvgSuccessfulCommitWriteDuration() ) );
        results.put("AVG_ABORTED_COMMIT_WRITE_DURATION (usec)", str( totalStats.evalAvgAbortedCommitWriteDuration() ) );
        results.put("AVG_COMMIT_WRITE_DURATION (usec)", str( totalStats.evalAvgCommitWriteDuration() ) );
        results.put("AVG_RD_SERVICE_TIME (usec)", str( totalStats.evalAvgRdServiceTime() ));
        results.put("AVG_WR_SERVICE_TIME (usec)", str( totalStats.evalAvgWrServiceTime() ));
        results.put("AVG_WR_INQUEUE_TIME (usec)", str( totalStats.evalAvgWrInQueueTime() ));
        results.put("AVG_RD_INQUEUE_TIME (usec)", str( totalStats.evalAvgRdInQueueTime() ));
        results.put("LOCAL_TIMEOUT", str( totalStats.get( StressorStats.LOCAL_TIMEOUT) ));
        results.put("REMOTE_TIMEOUT", str( totalStats.get( StressorStats.REMOTE_TIMEOUT) ));
        results.put("AVG_BACKOFF", str( totalStats.evalAvgBackoff() ));
        results.put("NumThreads", str(numOfThreads));

        fillMapWithExtraStats(totalStats, results);

        double cpu = 0, mem = 0;
        if (statSampler != null) {
            cpu = statSampler.getAvgCpuUsage();
            mem = statSampler.getAvgMemUsage();
        }
        results.put("CPU_USAGE", str(cpu));
        results.put("MEMORY_USAGE", str(mem));
        results.putAll(cacheWrapper.getAdditionalStats());

        saveSamples();

        log.info("Sending map to master " + results.toString());
        log.info("Finished generating report. Nr of failed operations on this node is: " + totalStats.get(StressorStats.NR_FAILURES) +
                ". Test duration is: " + Utils.getMillisDurationString(System.currentTimeMillis() - startTime));
        return results;
    }

    protected String str(Object o) {
        return String.valueOf(o);
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

        if (workloadGenerator.getSystemType().compareTo(AbstractWorkloadGenerator.SystemType.OPEN) != 0) {
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

    public final synchronized int getNumberOfActiveThreads() {
        int count = 0;
        for (Consumer stressor : consumers) {
            if (stressor.isActive()) {
                count++;
            }
        }
        return count;
    }

    public final synchronized int getNumberOfThreads() {
        return consumers.size();
    }

    public final synchronized void setNumberOfRunningThreads(int numOfThreads) {
        if (numOfThreads < 1 || !running.get()) {
            return;
        }
        Iterator<T> iterator = consumers.iterator();
        while (numOfThreads > 0 && iterator.hasNext()) {
            Consumer consumer = iterator.next();
            if (!consumer.isActive()) {
                consumer.active();
            }
            numOfThreads--;
        }

        if (numOfThreads > 0) {
            int threadIdx = consumers.size();
            while (numOfThreads-- > 0) {
                T consumer = createConsumer(threadIdx++);
                consumer.start();
                consumers.add(consumer);
            }
        } else {
            while (iterator.hasNext()) {
                iterator.next().inactive();
            }
        }
    }

    protected void saveSamples() {
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

    public void destroy() throws Exception {
        log.warn("Attention: going to destroy the wrapper");
        cacheWrapper.empty();
        cacheWrapper = null;
    }

    public synchronized final void stopBenchmark() {
        this.stoppedByJmx = true;
        finishBenchmarkTimer.cancel();
        finishBenchmark();
    }



    /* ******************** */
    /* *** GETTER/SETTER ** */
    /* ******************** */

    public void setNodeIndex(int nodeIndex) { this.nodeIndex = nodeIndex; }

    public void setNumOfThreads(int numOfThreads) { this.numOfThreads = numOfThreads; }

    public void setNumSlaves(int value) { this.numSlaves = value; }

    public void setPerThreadSimulTime(long perThreadSimulTime) { this.perThreadSimulTime = perThreadSimulTime; }

    public void setRetryOnAbort(boolean retryOnAbort) { this.retryOnAbort = retryOnAbort; }

    public void setRetrySameXact(boolean b) { this.retrySameXact = b; }

    public void setBackOffTime(long backOffTime) { this.backOffTime = backOffTime; }

    public void setStatsSamplingInterval(long statsSamplingInterval) { this.statsSamplingInterval = statsSamplingInterval; }



   /* ************************************************************************************************************* */
   /* ********************************************** INNER CLASSES ************************************************ */
   /* ************************************************************************************************************* */


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



    public class Consumer extends Thread {
        protected int threadIndex;
        //private double arrivalRate;

        public long commit_start = 0L;

        private boolean running = true;

        private boolean active = true;

        boolean measureCommitTime = false;

        boolean takeStats;

        private ProducerRate backOffSleeper;

        public S stats;


        /* ******************* */
        /* *** CONSTRUCTOR *** */
        /* ******************* */

        public Consumer(int threadIndex) {
            super("Stressor-" + threadIndex);
            stats = createStatsContainer();
            if (backOffTime > 0)
                try {
                    this.backOffSleeper =
                            ProducerRate.createInstance(workloadGenerator.getRateDistribution(), Math.pow((double) backOffTime, -1D));
                } catch (ProducerRate.ProducerRateException e) {
                    throw new RuntimeException(e);
                }
        }

        /* *************** */
        /* *** METHODS *** */
        /* *************** */

        private Transaction regenerate(Transaction oldTransaction, int threadIndex, boolean lastSuccessful) {

            if (!lastSuccessful && !retrySameXact) {
                this.backoffIfNecessary();
                Transaction newTransaction = generateTransaction(new RequestType(System.nanoTime(), oldTransaction.getType()), threadIndex);
                log.info("Thread " + threadIndex + ": regenerating a transaction of type " + oldTransaction.getType() +
                        " into a transaction of type " + newTransaction.getType());
                return newTransaction;
            }
            //If this is the first time xact runs or exact retry on abort is enabled...
            return oldTransaction;
        }

        protected boolean processTransaction(CacheWrapper wrapper, Transaction tx) {
            boolean successful = true;

            tx = regenerate(tx, threadIndex, successful);

            /* Start execution */
            cacheWrapper.startTransaction();
            try {
                tx.executeTransaction(cacheWrapper);
                log.info("Thread " + threadIndex + " successfully completed locally a transaction of type " +
                        tx.getType() + " btw, successful is " + successful);
            } catch (Throwable e) {
                successful = false;
                if (log.isDebugEnabled()) {
                    log.debug("Exception while executing transaction.", e);
                } else {
                    log.warn("Exception while executing transaction of type: " + tx.getType() + " " + e.getMessage());
                }
                /**         TODO spostalo nel consumer del tpcc
                 if (e instanceof ElementNotFoundException) {
                 stats.incAppFailures();
                 elementNotFoundExceptionThrown = true;
                 }
                 **/
                if (cacheWrapper.isTimeoutException(e)) {
                    stats.inc(StressorStats.LOCAL_TIMEOUT);
                }
            }
            //here we try to finalize the transaction
            //if any read/write has failed we abort

            /* In our tests we are interested in the commit time spent for write txs */
            if (successful && !tx.isReadOnly()) {
                commit_start = System.nanoTime();
                measureCommitTime = true;
            }
            try {
                cacheWrapper.endTransaction(successful, threadIndex);
                if (successful) {
                    log.info("Thread " + threadIndex + " successfully completed remotely a transaction of type " +
                            tx.getType() + " Btw, successful is " + successful);
                } else {
                    stats.inc(StressorStats.NR_FAILURES);
                    if (!tx.isReadOnly()) {
                        stats.inc(StressorStats.NR_WR_FAILURES);
                        /**    TODO spostalo nel consumer del tpcc
                         if (transaction instanceof NewOrderTransaction) {
                         nrNewOrderFailures++;
                         } else if (transaction instanceof PaymentTransaction) {
                         nrPaymentFailures++;
                         }
                         **/
                    } else {
                        stats.inc(StressorStats.NR_RD_FAILURES);
                    }

                }
            } catch (Throwable rb) {
                stats.inc(StressorStats.NR_FAILURES);
                stats.inc(StressorStats.REMOTE_TIMEOUT);
                successful = false;
                if (!tx.isReadOnly()) {
                    stats.inc(StressorStats.NR_WR_FAILURES);
                    stats.inc(StressorStats.NR_WR_FAILURES_ON_COMMIT);
                    /**     TODO spostalo nel consumer del tpcc
                     if (transaction instanceof NewOrderTransaction) {
                     nrNewOrderFailures++;
                     } else if (transaction instanceof PaymentTransaction) {
                     nrPaymentFailures++;
                     }
                     **/
                } else {
                    stats.inc(StressorStats.NR_RD_FAILURES);
                }
                if (log.isDebugEnabled()) {
                    log.debug("Error while committing", rb);
                } else {
                    log.warn("Error while committing: " + rb.getMessage());
                }
            }

            log.info("Successful = " + successful);
            return successful;
        }

        protected void queueStats(RequestType r, Transaction t) {

            if (t.isReadOnly()) {
                stats.inc(StressorStats.NUM_READ_DEQUEUED);
                stats.put(StressorStats.READ_IN_QUEUE_TIME, r.dequeueTimestamp - r.timestamp);
            } else {
                stats.inc(StressorStats.NUM_WRITE_DEQUEUED);
                stats.put(StressorStats.WRITE_IN_QUEUE_TIME,r.dequeueTimestamp - r.timestamp);
            }
        }

        @Override
        public void run() {
            long startService = System.nanoTime();      /* timestamp prima dell'esecuzione della tx */
            long start = -1;                            /* timestamp dopo la generazione della tx */
            long end;                                   /* timestamp dopo l'esecuzione della tx */
            boolean successful = true;

            Transaction tx;

            try {
                startPoint.await();
                log.info("Starting thread: " + getName());
            } catch (InterruptedException e) {
                log.warn("Interrupted while waiting for starting in " + getName());
            }

            while (assertRunning()) {
                tx = null;
                start = -1;
                if (workloadGenerator.getSystemType().compareTo(AbstractWorkloadGenerator.SystemType.OPEN) == 0) {  //Open system
                    try {
                        RequestType request = queue.take();
                        request.dequeueTimestamp = System.nanoTime();

                        tx = generateTransaction(request, threadIndex);

                        // if PassiveReplication so skip whether:
                        // a) master node && readOnly transaction
                        // b) slave node && write transaction
                        boolean masterAndReadOnlyTx = cacheWrapper.isTheMaster() && tx.isReadOnly();
                        boolean slaveAndWriteTx = (!cacheWrapper.isTheMaster() && !tx.isReadOnly());

                        if (cacheWrapper.isPassiveReplication() && (masterAndReadOnlyTx || slaveAndWriteTx)) {
                            continue;
                        }
                        start = request.timestamp;

                        /* updating queue stats */
                        queueStats(request, tx);
                    } catch (InterruptedException ir) {
                        log.error("»»»»»»»THREAD INTERRUPTED WHILE TRYING GETTING AN OBJECT FROM THE QUEUE«««««««");
                    }
                } else {
                    tx = choiceTransaction(cacheWrapper.isPassiveReplication(), cacheWrapper.isTheMaster(), threadIndex);
                    log.info("Closed system: starting a brand new transaction of type " + tx.getType());
                }


                startService = System.nanoTime();

                do {
                    successful = processTransaction(cacheWrapper, tx);
                }
                //If we experience an elementNotFoundException we do not want to restart the very same xact!!
                //If a xact is not progressing at the end of the test, we kill it. Some stats will be slightly affected by this
                while (assertRunning() && retryOnAbort && !successful);

                end = System.nanoTime();

                if (workloadGenerator.getSystemType().compareTo(AbstractWorkloadGenerator.SystemType.MULE) == 0) {  //Closed system   --> no queueing time
                    start = startService;
                }

                if (!tx.isReadOnly()) {
                    stats.put(StressorStats.WRITE_DURATION, end-start);
                    stats.put(StressorStats.WRITE_SERVICE_TIME, end - startService);
                    /*       TODO sposta in tpcc stressor
                    if (transaction instanceof NewOrderTransaction) {
                        newOrderDuration += end - start;
                        newOrderServiceTime += end - startService;
                    } else if (transaction instanceof PaymentTransaction) {
                        paymentDuration += end - start;
                        paymentServiceTime += end - startService;
                    }
                    */
                    if (successful) {
                        stats.put(StressorStats.SUCCESSFUL_WRITE_DURATION, end-startService);
                        stats.inc(StressorStats.WRITES);
                        /*      TODO sposta in tpcc stressor
                        if (transaction instanceof PaymentTransaction) {
                            stats.incPayment();
                        } else if (transaction instanceof NewOrderTransaction) {
                            newOrder++;
                        }
                        */
                    }
                } else {
                    stats.put(StressorStats.READ_DURATION, end - start);
                    stats.put(StressorStats.READ_SERVICE_TIME, end - startService);

                    if (successful) {
                        stats.inc(StressorStats.READS);
                        stats.put(StressorStats.SUCCESSFUL_READ_DURATION, end - startService);
                    }
                }

                if (measureCommitTime) {    //We sample just the last commit time, i.e., the successful one
                    if (successful) {
                        stats.put(StressorStats.SUCCESSFUL_COMMIT_WRITE_DURATION, end - commit_start);
                    } else {
                        stats.put(StressorStats.ABORTED_COMMIT_WRITE_DURATION, end - commit_start);

                    }
                    stats.put(StressorStats.COMMIT_WRITE_DURATION, end - commit_start);
                }

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



    /* ************************ */
    /* *** RequestType CLASS ** */
    /* ************************ */

    protected class RequestType {

        public long timestamp;
        public long dequeueTimestamp;
        public int transactionType;

        public RequestType(long timestamp, int transactionType) {
            this.timestamp = timestamp;
            this.transactionType = transactionType;
        }

    }
}
