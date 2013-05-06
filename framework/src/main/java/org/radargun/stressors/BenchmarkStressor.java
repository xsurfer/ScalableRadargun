package org.radargun.stressors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.CacheWrapper;
import org.radargun.stressors.consumer.Consumer;
import org.radargun.stressors.producer.*;
import org.radargun.stages.AbstractBenchmarkStage;
import org.radargun.stressors.commons.StressorStats;
import org.radargun.utils.StatSampler;
import org.radargun.utils.Utils;
import org.radargun.workloadGenerator.*;
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
 *         E-mail: perfabio87@gmail.com
 *         Date: 4/1/13
 */
public class BenchmarkStressor extends AbstractCacheWrapperStressor implements Observer {

    /* **************** */
    /* *** COSTANTS *** */
    /* **************** */

    //in milliseconds, each producer sleeps for this time in average
    public static final int AVERAGE_PRODUCER_SLEEP_TIME = 10;



    /* ****************** */
    /* *** ATTRIBUTES *** */
    /* ****************** */

    private static Log log = LogFactory.getLog(AbstractCacheWrapperStressor.class);

    public AtomicLong countJobs = new AtomicLong(0L);

    protected final AtomicBoolean running = new AtomicBoolean(true);

    /**
     * last arrival rate of the transactions to the system (transactions per second)
     */
    protected int lastArrivalRate = 0;



    protected StatSampler statSampler;

    protected final Timer finishBenchmarkTimer = new Timer("Finish-Benchmark-Timer");

    protected long startTime;

    protected long endTime;

    protected volatile CountDownLatch startPoint;

    protected boolean stoppedByJmx = false;

    protected BlockingQueue<RequestType> queue = new LinkedBlockingQueue<RequestType>();



    protected final List<Producer> producers = Collections.synchronizedList(new ArrayList<Producer>());

    protected final List<Consumer> consumers = Collections.synchronizedList(new LinkedList<Consumer>());

    protected AbstractBenchmarkStage benchmarkStage;

    protected SystemType system;

    protected StressorParameter parameters;



    /* ******************* */
    /* *** CONSTRUCTOR *** */
    /* ******************* */

    public BenchmarkStressor(CacheWrapper cacheWrapper,
                             AbstractBenchmarkStage benchmarkStage,
                             SystemType system,
                             StressorParameter parameters) {

        if (cacheWrapper == null) { throw new IllegalStateException("Null wrapper not allowed"); }

        this.cacheWrapper = cacheWrapper;
        this.benchmarkStage = benchmarkStage;
        this.system = system;
        this.parameters = parameters;

    }



    /* ******************* */
    /* *** TO OVERRIDE *** */
    /* ******************* */

    //protected abstract Map<String, String> processResults(List<T> stressors);

    //protected abstract void extractExtraStats(S totalStats, S singleStats);

    //protected abstract void fillMapWithExtraStats(S totalStats, Map<String, String> results);



    /* ****************** */
    /* ***** METHODS **** */
    /* ****************** */

    public void addToQueue(RequestType r){
        queue.offer(r);
    }

    public RequestType takeFromQueue(){
        RequestType ret = null;
        try {
            ret = queue.take();
        } catch (InterruptedException e) {
            log.error("»»»»»»»THREAD INTERRUPTED WHILE TRYING GETTING AN OBJECT FROM THE QUEUE«««««««");
        }
        return ret;
    }

    public final Map<String, String> stress(){
        return system.stress(this);
    }

    public final Map<String, String> stress(OpenSystem system){
        cacheWrapper.addObserver(this);

        log.trace("Registring this Stressor to the WorkLoadGenerator (Observable)");
        system.getWorkloadGenerator().addObserver(this);

        benchmarkStage.validateTransactionsWeight();
        benchmarkStage.initialization();
        if( initBenchmarkTimer() ){
            if ( parameters.getStatsSamplingInterval() > 0 ) {
                statSampler = new StatSampler( parameters.getStatsSamplingInterval() );
            }
            log.info("Executing: " + this.toString());

            log.info("Open System");
            log.info("MaxArrivalRate: " + system.getWorkloadGenerator().getMaxArrivalRate());
            log.info("Granularity: " + system.getWorkloadGenerator().getGranularity());
            log.info("InitTime: " + system.getWorkloadGenerator().getInitTime());

            log.trace("Starting the workload generator");
            system.getWorkloadGenerator().start();

            log.trace("Sampler started");
            if (statSampler != null) { statSampler.start(); }

            executeOperations(system);

            if (statSampler != null) {
                statSampler.cancel();
            }
            return processResults(consumers);
        } else {
            log.warn("Execution time <= 0. This slave will execute on next stage");
            return null;
        }
    }

    /**
     * Stressor method for closed system
     * @param system
     * @return
     */
    public final Map<String, String> stress(ClosedSystem system ){
        this.system = system;
        return null;
    }

    /**
     * Stressor method for mule system. It doesn't use producer-consumer
     * @param system
     * @return
     */
    public final Map<String, String> stress(MuleSystem system){
        this.system = system;
        return null;
    }


    /**
     *
     * @return true if timer is setted, false otherwise
     */
    private boolean initBenchmarkTimer() {
        if (parameters.getPerThreadSimulTime() > 0) {
            finishBenchmarkTimer.schedule(new TimerTask() {
                @Override
                public void run() {
                    finishBenchmark();
                }
            }, parameters.getPerThreadSimulTime() * 1000);
            return true;
        }
        return false;
    }

    protected void executeOperations(SystemType system) {

        startPoint = new CountDownLatch(1);
        for (int threadIndex = 0; threadIndex < parameters.getNumOfThreads(); threadIndex++) {
            Consumer consumer = system.createConsumer(cacheWrapper, threadIndex, benchmarkStage, this, parameters);
            consumers.add(consumer);
            consumer.start();
        }
        log.info("Cache wrapper info is: " + cacheWrapper.getInfo());
        startTime = System.currentTimeMillis();
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
            system.finishBenchmark(this);
        }
    }

    public void finishBenchmark(OpenSystem system) {
        stopConsumers();
        /* stoppo i producer */
        log.trace("Stopping producers");
        stopProducers();

        log.trace("Stopping workload generator");
        /* stoppo il workload generator */
        system.getWorkloadGenerator().stop();

        log.trace("Waking up waiting thread");
        notifyAll();
    }

    public void finishBenchmark(ClosedSystem system) {
        stopConsumers();
        /* stoppo i producer */
        log.trace("Stopping producers");
        stopProducers();

        log.trace("Waking up waiting thread");
        notifyAll();
    }

    public void finishBenchmark(MuleSystem system) {
        stopConsumers();

        log.trace("Waking up waiting thread");
        notifyAll();
    }

    private void stopConsumers() {
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

    private void updateProducer(OpenSystem system) {
        synchronized (running) {
            if (running.get()) {
                log.info("Stopping old producer");
                stopProducers();

                synchronized (producers) {
                    producers.clear();
                    producers.addAll(system.createProducers(cacheWrapper, benchmarkStage, this, parameters ));
                }

                log.info("Starting " + producers.size() + " producers");
                startProducers();
            }
        }
    }


    //public abstract StressorStats createStatsContainer();

    protected Map<String, String> processResults(List<Consumer> consumers) {

        long duration = 0;
        StressorStats totalStats = new StressorStats(); //createStatsContainer();

        /* 1) Extracting per consumer stats */
        for (Consumer consumer : consumers) {

            StressorStats singleStats = consumer.stats;

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

            //extractExtraStats(totalStats, singleStats);

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
        results.put("DURATION (msec)", str(totalStats.get(StressorStats.DURATION)));
        results.put("REQ_PER_SEC", str(totalStats.evalRequestPerSec()));
        results.put("READS_PER_SEC", str(totalStats.evalRdPerSec()));
        results.put("WRITES_PER_SEC", str(totalStats.evalWrtPerSec()));
        results.put("READ_COUNT", str(totalStats.get(StressorStats.READS)));
        results.put("WRITE_COUNT", str(totalStats.get(StressorStats.WRITES)));
        results.put("FAILURES", str(totalStats.get(StressorStats.NR_FAILURES)));
        results.put("APPLICATION_FAILURES", str(totalStats.get(StressorStats.APP_FAILURES)));
        results.put("WRITE_FAILURES", str(totalStats.get(StressorStats.NR_WR_FAILURES)));
        results.put("READ_FAILURES", str(totalStats.get(StressorStats.NR_RD_FAILURES)));
        results.put("AVG_SUCCESSFUL_DURATION (usec)", str(totalStats.evalAvgSuccessfulDuration()));
        results.put("AVG_SUCCESSFUL_READ_DURATION (usec)", str(totalStats.evalAvgSuccessfulReadDuration()));
        results.put("AVG_SUCCESSFUL_WRITE_DURATION (usec)", str(totalStats.evalAvgSuccessfulWriteDuration()));
        results.put("AVG_SUCCESSFUL_COMMIT_WRITE_DURATION (usec)", str(totalStats.evalAvgSuccessfulCommitWriteDuration()));
        results.put("AVG_ABORTED_COMMIT_WRITE_DURATION (usec)", str(totalStats.evalAvgAbortedCommitWriteDuration()));
        results.put("AVG_COMMIT_WRITE_DURATION (usec)", str(totalStats.evalAvgCommitWriteDuration()));
        results.put("AVG_RD_SERVICE_TIME (usec)", str(totalStats.evalAvgRdServiceTime()));
        results.put("AVG_WR_SERVICE_TIME (usec)", str(totalStats.evalAvgWrServiceTime()));
        results.put("AVG_WR_INQUEUE_TIME (usec)", str(totalStats.evalAvgWrInQueueTime()));
        results.put("AVG_RD_INQUEUE_TIME (usec)", str(totalStats.evalAvgRdInQueueTime()));
        results.put("LOCAL_TIMEOUT", str(totalStats.get(StressorStats.LOCAL_TIMEOUT)));
        results.put("REMOTE_TIMEOUT", str(totalStats.get(StressorStats.REMOTE_TIMEOUT)));
        results.put("AVG_BACKOFF", str(totalStats.evalAvgBackoff()));
        results.put("NumThreads", str(parameters.getNumOfThreads()));

        //fillMapWithExtraStats(totalStats, results);

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
     * This method is executed if there is a change in the arrival rate or in the cluster's size
     *
     * @param o
     * @param arg
     */
    @Override
    public final void update(Observable o, Object arg) {
        if(system.getType().equals(SystemType.OPEN)){
            // potrebbe essere cambiato l'arrival rate e/o la dimensione del cluster
            // aggiorno i producer

            Integer cmd = (Integer) arg;

            switch (cmd) {
                case CacheWrapper.VIEW_CHANGED:
                    log.info("VIEW has changed: #slaves = " + cacheWrapper.getNumMembers());
                    updateProducer( (OpenSystem) system );
                    break;
                case AbstractWorkloadGenerator.ARRIVAL_RATE_CHANGED:
                    log.info("Arrival rate changed");
                    if ( ((OpenSystem) system).getWorkloadGenerator().getArrivalRate() != this.lastArrivalRate) {
                        this.lastArrivalRate = ((OpenSystem) system).getWorkloadGenerator().getArrivalRate();
                        updateProducer( (OpenSystem) system );
                    }
                    break;
                default:
                    log.warn("Unrecognized argument");
                    break;
            }
        } else {
            // è cambiata solo la dimensione del cluster
            // i producer (se presenti) non sono da cambiare
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
        Iterator<Consumer> iterator = consumers.iterator();
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
                Consumer consumer = system.createConsumer(cacheWrapper, threadIdx++, benchmarkStage, this, parameters);
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
        log.info("Saving samples in the file sample-" + parameters.getNodeIndex());
        File f = new File("sample-" + parameters.getNodeIndex());
        try {
            BufferedWriter bw = new BufferedWriter(new FileWriter(f));
            List<Long> mem = statSampler.getMemoryUsageHistory();
            List<Double> cpu = statSampler.getCpuUsageHistory();

            int size = Math.min(mem.size(), cpu.size());
            bw.write("#Time (milliseconds)\tCPU(%)\tMemory(bytes)");
            bw.newLine();
            for (int i = 0; i < size; ++i) {
                bw.write((i * parameters.getStatsSamplingInterval()) + "\t" + cpu.get(i) + "\t" + mem.get(i));
                bw.newLine();
            }
            bw.flush();
            bw.close();
        } catch (IOException e) {
            log.warn("IOException caught while saving sampling: " + e.getMessage());
        }
    }

    @Override
    @Deprecated
    public Map<String, String> stress(CacheWrapper wrapper) {
        throw new IllegalStateException("Use other stress methods");
        //return null;
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


}
