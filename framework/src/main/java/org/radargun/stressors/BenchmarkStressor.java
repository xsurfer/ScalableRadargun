package org.radargun.stressors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.CacheWrapper;
import org.radargun.stressors.consumer.ClosedConsumer;
import org.radargun.stressors.consumer.Consumer;
import org.radargun.stressors.consumer.MuleConsumer;
import org.radargun.stressors.consumer.OpenConsumer;
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
    protected static final int AVERAGE_PRODUCER_SLEEP_TIME = 10;



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
                             StressorParameter parameters) {

        if (cacheWrapper == null) { throw new IllegalStateException("Null wrapper not allowed"); }

        this.cacheWrapper = cacheWrapper;
        this.benchmarkStage = benchmarkStage;
        this.parameters = parameters;

        //log.trace("Registring this TpccStressor to the WorkLoadGenerator (Observable)");
        //this.workloadGenerator.addObserver(this);
    }



    /* ******************* */
    /* *** TO OVERRIDE *** */
    /* ******************* */

    //protected abstract OpenConsumer createConsumer(int threadIndex);

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


    public final Map<String, String> stress(OpenSystem system ){

        cacheWrapper.addObserver(this);

        benchmarkStage.validateTransactionsWeight();
        benchmarkStage.initialization();

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

        executeOperations();

        if (statSampler != null) {
            statSampler.cancel();
        }
        return processResults(consumers);
    }

    /**
     * Stressor method for closed system
     * @param system
     * @return
     */
    public final Map<String, String> stress(ClosedSystem system ){

        return null;
    }


    /**
     * Stressor method for mule system. It doesn't use producer-consumer
     * @param system
     * @return
     */
    public final Map<String, String> stress(MuleSystem system){

        return null;
    }


    private void initBenchmarkTimer() throws Exception {
        if (parameters.perThreadSimulTime > 0) {
            finishBenchmarkTimer.schedule(new TimerTask() {
                @Override
                public void run() {
                    finishBenchmark();
                }
            }, parameters.perThreadSimulTime * 1000);
        } else if (parameters.perThreadSimulTime <= 0) {
            throw new Exception("Slave arrived too late, it will works on the next stage");
        }
    }

    private Consumer createConsumer(int threadIndex, SystemType system){
        Consumer consumer = null;
        if(system.getType().equals(SystemType.OPEN)){
            consumer = new OpenConsumer(cacheWrapper,
                                        threadIndex,
                                        (OpenSystem) system,
                                        benchmarkStage,
                                        this,
                                        parameters
                                        );
        } else if(system.getType().equals(SystemType.CLOSED)){
            consumer = new ClosedConsumer(cacheWrapper,
                                        threadIndex,
                                        (ClosedSystem) system,
                                        benchmarkStage,
                                        this,
                                        parameters
                                        );
        }
        else {
            consumer = new MuleConsumer(cacheWrapper,
                                        threadIndex,
                                        (MuleSystem) system,
                                        benchmarkStage,
                                        this,
                                        parameters
                                        );
        }
        return consumer;
    }

    protected void executeOperations(SystemType system) {

        startPoint = new CountDownLatch(1);
        for (int threadIndex = 0; threadIndex < parameters.numOfThreads; threadIndex++) {
            Consumer consumer = createConsumer(threadIndex, system);
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
            stopStressor();

            /* stoppo i producer nel caso di sistema a producer */
            if(system.getType())
            if (workloadGenerator.getSystemType().compareTo(AbstractWorkloadGenerator.SystemType.OPEN) == 0) {
                log.trace("Stopping producers");
                stopProducers();
            }
            log.trace("Stopping workload generator");
            /* stoppo il workload generator nel caso di sistema aperto */
            workloadGenerator.stop();
            log.trace("Waking up waiting thread");
            notifyAll();
        }
    }

    private void stopStressor() {
        synchronized (consumers) {
            for (OpenConsumer stressor : consumers) {
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

                synchronized (producers) {
                    producers.clear();

                    producers.addAll(workloadGenerator.createProducers(cacheWrapper, nodeIndex, getWriteWeight(), getReadWeight() ));
//                    for (int i = 0; i < producerRates.length; ++i) {
//                        producers.add( i, new BenchmarkStressor.ClosedProducer(getThinkTime(), nodeIndex) );
//                        //producers[i] = new Producer(producerRates[i], i);
//                    }
                }

                workloadGenerator.createProducers(cacheWrapper,nodeIndex,getWriteWeight(),getReadWeight());


                //createProducers(workloadGenerator.getArrivalRate());
                log.info("Starting " + producers.size() + " producers");
                startProducers();
            }
        }
    }


    //public abstract StressorStats createStatsContainer();

    protected Map<String, String> processResults(List<OpenConsumer> consumers) {

        long duration = 0;
        StressorStats totalStats = new StressorStats(); //createStatsContainer();

        /* 1) Extracting per consumer stats */
        for (OpenConsumer consumer : consumers) {

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
        results.put("NumThreads", str(numOfThreads));

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


    /* crea il numero di producer sul singolo nodo a seconda della size della popolazione */
    private void createProducers(ClosedSystem system){

    }


    /**
     * Class in charge of create or update the Producer in base of arrival rate.
     *
     * @param system
     */
    private void createProducers(OpenSystem system) {

        log.info("Creating/Updating producers");

        ProducerRate[] producerRates;
        if (cacheWrapper.isPassiveReplication()) {
            if (cacheWrapper.isTheMaster()) {
                log.info("Creating producers groups for the master. Write transaction percentage is " + getWriteWeight());
                producerRates = new GroupProducerRateFactory(system.getWorkloadGenerator().getRateDistribution(),
                        getWriteWeight(),
                        1,
                        nodeIndex,
                        AVERAGE_PRODUCER_SLEEP_TIME).create();
            } else {
                log.info("Creating producers groups for the slave. Read-only transaction percentage is " + getReadWeight());
                producerRates = new GroupProducerRateFactory(system.getWorkloadGenerator().getRateDistribution(),
                        getReadWeight(),
                        cacheWrapper.getNumMembers() - 1,
                        nodeIndex == 0 ? nodeIndex : nodeIndex - 1,
                        AVERAGE_PRODUCER_SLEEP_TIME).create();
            }
        } else {
            log.info("Creating producers groups");
            producerRates = new GroupProducerRateFactory(system.getWorkloadGenerator().getRateDistribution(), system.getWorkloadGenerator().getArrivalRate(), cacheWrapper.getNumMembers(), nodeIndex,
                    AVERAGE_PRODUCER_SLEEP_TIME).create();
        }
        //producers = new Producer[producerRates.length];

        synchronized (producers) {
            producers.clear();
            for (int i = 0; i < producerRates.length; ++i) {
                producers.add(i, new OpenProducer(this, producerRates[i], i));
                //producers[i] = new Producer(producerRates[i], i);
            }
        }
    }

    public final synchronized int getNumberOfActiveThreads() {
        int count = 0;
        for (OpenConsumer stressor : consumers) {
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
        Iterator<OpenConsumer> iterator = consumers.iterator();
        while (numOfThreads > 0 && iterator.hasNext()) {
            OpenConsumer consumer = iterator.next();
            if (!consumer.isActive()) {
                consumer.active();
            }
            numOfThreads--;
        }

        if (numOfThreads > 0) {
            int threadIdx = consumers.size();
            while (numOfThreads-- > 0) {
                OpenConsumer consumer = createConsumer(threadIdx++);
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
