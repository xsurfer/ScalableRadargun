package org.radargun.stages.stressors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.CacheWrapper;
import org.radargun.TransactionFactory;
import org.radargun.stages.AbstractBenchmarkStage;
import org.radargun.stages.stressors.commons.StressorStats;
import org.radargun.stages.stressors.consumer.Consumer;
import org.radargun.stages.stressors.producer.ClosedProducer;
import org.radargun.stages.stressors.producer.GroupProducerRateFactory;
import org.radargun.stages.stressors.producer.OpenProducer;
import org.radargun.stages.stressors.producer.Producer;
import org.radargun.stages.stressors.producer.ProducerRate;
import org.radargun.stages.stressors.producer.RequestType;
import org.radargun.stages.stressors.systems.ClosedSystem;
import org.radargun.stages.stressors.systems.IProducerSystem;
import org.radargun.stages.stressors.systems.MuleSystem;
import org.radargun.stages.stressors.systems.OpenSystem;
import org.radargun.stages.stressors.systems.System;
import org.radargun.stages.stressors.systems.workloadGenerators.AbstractWorkloadGenerator;
import org.radargun.utils.StatSampler;
import org.radargun.utils.Utils;
import org.radargun.utils.WorkerThreadFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Fabio Perfetti E-mail: perfabio87@gmail.com Date: 4/1/13
 */
public abstract class AbstractBenchmarkStressor<T extends Parameters, S extends Consumer, V extends Producer, Z extends TransactionFactory>
      extends AbstractCacheWrapperStressor implements Observer {

    /* **************** */
    /* *** COSTANTS *** */
    /* **************** */

   //in milliseconds, each producer sleeps for this time in average
   public static final int AVERAGE_PRODUCER_SLEEP_TIME = 10;


    /* ***************** */
    /* *** EXECUTORS *** */
    /* ***************** */

   ExecutorService consumerExecutorService;

   ExecutorService producersExecutorService;

    /* ****************** */
    /* *** ATTRIBUTES *** */
    /* ****************** */

   private static Log log = LogFactory.getLog(AbstractBenchmarkStressor.class);

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

   protected final BlockingQueue<RequestType> queue = new LinkedBlockingQueue<RequestType>();

   protected final List<V> producers = Collections.synchronizedList(new ArrayList<V>());

   protected final List<S> consumers = Collections.synchronizedList(new LinkedList<S>());

   protected final AbstractBenchmarkStage benchmarkStage;

   protected final System system;

   protected final T parameters;



    /* ******************* */
    /* *** CONSTRUCTOR *** */
    /* ******************* */

   public AbstractBenchmarkStressor(CacheWrapper cacheWrapper,
                                    AbstractBenchmarkStage benchmarkStage,
                                    System system,
                                    T parameters) {

      if (cacheWrapper == null) {
         throw new IllegalStateException("Null wrapper not allowed");
      }

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

   protected abstract void initialization();

   protected abstract void validateTransactionsWeight();

   //public abstract int nextTransaction(int threadIndex);

   //public abstract ITransaction generateTransaction(RequestType type, int threadIndex);

   //public abstract ITransaction choiceTransaction(boolean isPassiveReplication, boolean isTheMaster, int threadId);

   protected abstract double getWriteWeight();

   protected abstract double getReadWeight();

   protected abstract Z createTransactionFactory(int threadIndex);

   protected abstract S createConsumer(int threadIndex);


    /* ****************** */
    /* ***** METHODS **** */
    /* ****************** */

   public void addToQueue(RequestType r) {
      queue.offer(r);
   }

   public RequestType takeFromQueue() {
      RequestType ret = null;
      try {
         ret = queue.take();
      } catch (InterruptedException e) {
         log.error("»»»»»»»THREAD INTERRUPTED WHILE TRYING GETTING AN OBJECT FROM THE QUEUE«««««««");
      }
      return ret;
   }

   public final Map<String, String> stress() {
      return system.stress(this);
   }

   public final Map<String, String> stress(OpenSystem system) {
      //cacheWrapper.addObserver(this);  This is not useful anymore. Slaves are notified by jmx

      log.trace("Registring this Stressor to the WorkLoadGenerator (Observable)");
      system.getWorkloadGenerator().addObserver(this);

      validateTransactionsWeight();
      initialization();
      if (initBenchmarkTimer()) {
         if (parameters.getStatsSamplingInterval() > 0) {
            statSampler = new StatSampler(parameters.getStatsSamplingInterval());
         }
         log.info("Executing: " + this.toString());

         log.info("Open System");
         log.info("MaxArrivalRate: " + system.getWorkloadGenerator().getMaxArrivalRate());
         log.info("Granularity: " + system.getWorkloadGenerator().getGranularity());
         log.info("FirstGranularity: " + system.getWorkloadGenerator().getFirstGranularity());
         log.info("InitTime: " + system.getWorkloadGenerator().getInitTime());

         log.trace("Starting the workload generator");
         system.getWorkloadGenerator().start();


         if (statSampler != null) {
            statSampler.start();
            log.trace("Sampler started");
         }

         executeOperations();

         log.info("Stopping Sampler");
         if (statSampler != null) {
            statSampler.cancel();
            log.info("Sampler Stopped");
         }
         return processResults(consumers);
      } else {
         log.warn("Execution time <= 0. This slave will execute on next stage");
         return null;
      }
   }

   /**
    * Stressor method for closed system
    *
    * @param system
    * @return
    */
   public final Map<String, String> stress(ClosedSystem system) {
      //cacheWrapper.addObserver(this);  This is not useful anymore. Slaves are notified by jmx

      validateTransactionsWeight();
      initialization();
      if (initBenchmarkTimer()) {
         if (parameters.getStatsSamplingInterval() > 0) {
            statSampler = new StatSampler(parameters.getStatsSamplingInterval());
         }

         log.info("Closed System");
         log.info("Executing: " + this.toString());

         log.info("Starting Producers");
         stopCreateStartProducers(system);

         if (statSampler != null) {
            statSampler.start();
            log.trace("Sampler started");
         }

         executeOperations();

         log.info("Stopping Sampler");
         if (statSampler != null) {
            statSampler.cancel();
            log.info("Sampler Stopped");
         }
         return processResults(consumers);
      } else {
         log.warn("Execution time <= 0. This slave will execute on next stage");
         return null;
      }
   }

   /**
    * Stressor method for mule system. It doesn't use producer-consumer
    *
    * @param system
    * @return
    */
   public final Map<String, String> stress(MuleSystem system) {

      validateTransactionsWeight();
      initialization();
      if (initBenchmarkTimer()) {
         if (parameters.getStatsSamplingInterval() > 0) {
            statSampler = new StatSampler(parameters.getStatsSamplingInterval());
         }

         log.info("Mule System");
         log.info("Executing: " + this.toString());

         if (statSampler != null) {
            statSampler.start();
            log.trace("Sampler started");
         }

         executeOperations();

         log.info("Stopping Sampler");
         if (statSampler != null) {
            statSampler.cancel();
            log.info("Sampler Stopped");
         }
         return processResults(consumers);
      } else {
         log.warn("Execution time <= 0. This slave will execute on next stage");
         return null;
      }
   }


   /**
    * @return true if timer is setted, false otherwise
    */
   private boolean initBenchmarkTimer() {
      if (parameters.getSimulationTimeSec() > 0) {
         finishBenchmarkTimer.schedule(new TimerTask() {
            @Override
            public void run() {
               finishBenchmark();
            }
         }, parameters.getSimulationTimeSec() * 1000);
         return true;
      }
      return false;
   }

   protected final void executeOperations() {

      startPoint = new CountDownLatch(1);
      parameters.setStartPoint(startPoint);

      consumerExecutorService = Executors.newFixedThreadPool(parameters.getNumOfThreads(), new WorkerThreadFactory("Consumer", false));


      for (int threadIndex = 0; threadIndex < parameters.getNumOfThreads(); threadIndex++) {
         S consumer = createConsumer(threadIndex);
         consumers.add(consumer);

         consumerExecutorService.execute(consumer);
      }
      log.info("Cache wrapper info is: " + cacheWrapper.getInfo());
      startTime = java.lang.System.currentTimeMillis();
      startPoint.countDown();
      blockWhileRunning();
      log.info("Shutting down consumerExecutorService");
      consumerExecutorService.shutdown();

      try {
         log.info("Waiting for the threadS in consumerExecutorService");
         if (consumerExecutorService.awaitTermination(60, TimeUnit.NANOSECONDS)) {
            log.info("Some producer still running...! Killing them...");
            List<Runnable> runnablesNotEnded = consumerExecutorService.shutdownNow();
            log.info("Here you have buggus threads:");
            for (Runnable runnable : runnablesNotEnded) {
               log.info(runnable.toString());
            }
         }
         log.info("All threads in consumerExecutorService have already finished");
      } catch (InterruptedException e) {
         throw new RuntimeException(e);
      }

      log.info("Fine executeOperations");
      endTime = java.lang.System.currentTimeMillis();
      //return consumers;
   }

   private synchronized void blockWhileRunning() {
      while (running.get()) {
         try {
            wait();
            log.info("Awake");
         } catch (InterruptedException e) {
            throw new RuntimeException(e);
         }
      }
   }

   protected final void finishBenchmark() {
      if (running.compareAndSet(true, false)) {
         system.finishBenchmark(this);
      }
   }

   public synchronized void finishBenchmark(OpenSystem system) {

      // TODO aspettare la fine delle tx in coda

      if (!running.get()) {
         log.trace("Stopping workload generator");
         system.getWorkloadGenerator().stop();

         log.trace("Stopping consumers");
         stopConsumers();

         log.trace("Stopping producers");
         stopProducers();

         log.trace("Waking up waiting thread");
         notifyAll();
      }
   }

   public synchronized void finishBenchmark(ClosedSystem system) {
      if (!running.get()) {
         log.trace("Stopping consumers");
         stopConsumers();

         log.trace("Stopping producers");
         stopProducers();

         log.trace("Waking up waiting thread");
         notifyAll();
      }
   }

   public synchronized void finishBenchmark(MuleSystem system) {
      if (!running.get()) {
         stopConsumers();

         log.trace("Waking up waiting thread");
         notifyAll();
      }
   }

   private void stopConsumers() {
      // Stoppare i consumer significa finire il benchmark
      if (!running.get()) {
         log.info("Stopping Consumers...");
         synchronized (consumers) {
            for (Consumer consumer : consumers) {
               consumer.finish();
            }
         }
      }
   }

   private void stopProducers() {
      if (running.get()) {
         log.info("Stopping Producers...");
         synchronized (producers) {
            for (Producer producer : producers) {
               producer.interrupt();
            }
         }
      }
   }

   private void stopCreateStartProducers(IProducerSystem system) {
      synchronized (running) {
         if (running.get()) {
            synchronized (producers) {
               if (producers != null && producers.size() > 0) {
                  stopProducers();
               }

               log.info("Creating new producers...");
               producers.clear();
               producers.addAll(system.createProducers(this));

               if (producers.size() > 0) {
                  log.info("Starting new (" + producers.size() + ") producers...");
                  producersExecutorService = Executors.newFixedThreadPool(producers.size(), new WorkerThreadFactory("Producer", false));
                  for (Producer producer : producers) {
                     log.info("STARTING: " + producer);
                     producersExecutorService.execute(producer);
                  }
                  producersExecutorService.shutdown();
               } else {
                  log.info("No producer to start!");
               }
            }
         }
      }
   }

//    private synchronized void updateProducer(IProducerSystem system) {
//        synchronized (running){
//            if (running.get()) {
//
//                }
//            }
//        }
//    }

   public synchronized List<Producer> createProducers(ClosedSystem system) {
      //throw new IllegalStateException("Still not implemented");

      log.info("Creating/Updating producers closed system");

      ProducerRate[] producerRates;
      if (cacheWrapper.isPassiveReplication()) {
         if (cacheWrapper.isTheMaster()) {
            log.info("Creating producers groups for the master. Write transaction percentage is " + getWriteWeight());
            //TODO rendere la distribuzione personalizzabile
                /*
                producerRates = new GroupProducerRateFactory(AbstractWorkloadGenerator.RateDistribution.EXPONENTIAL,
                        getWriteWeight(),
                        1,
                        parameters.getNodeIndex(),
                        AbstractBenchmarkStressor.AVERAGE_PRODUCER_SLEEP_TIME).create();
                */

            throw new IllegalStateException("NOT YET IMPLEMENTED");
            //producerRates = GroupProducerRateFactory.createClients();

         } else {
            log.info("Creating producers groups for the slave. Read-only transaction percentage is " + getReadWeight());
                /*
                producerRates = new GroupProducerRateFactory(AbstractWorkloadGenerator.RateDistribution.EXPONENTIAL,
                        getReadWeight(),
                        cacheWrapper.getNumMembers() - 1,
                        parameters.getNodeIndex() == 0 ? parameters.getNodeIndex() : parameters.getNodeIndex() - 1,
                        AbstractBenchmarkStressor.AVERAGE_PRODUCER_SLEEP_TIME).create();
                */

            throw new IllegalStateException("NOT YET IMPLEMENTED");
            //producerRates = GroupProducerRateFactory.createClients();
         }
      } else {
         log.info("Creating producers groups");
            /*
            producerRates = new GroupProducerRateFactory(AbstractWorkloadGenerator.RateDistribution.EXPONENTIAL,
                    system.getWorkloadGenerator().arrivalRate(),
                    cacheWrapper.getNumMembers(),
                    parameters.getNodeIndex(),
                    AbstractBenchmarkStressor.AVERAGE_PRODUCER_SLEEP_TIME).createClients();
            */

         producerRates = GroupProducerRateFactory.createClients(system.getPopulation(),
                                                                system.getRateDistribution(),
                                                                cacheWrapper.getNumMembers(),
                                                                parameters.getNodeIndex(),
                                                                system.getThinkTime());

      }

      List<Producer> producers = new ArrayList<Producer>();
      for (int i = 0; i < producerRates.length; ++i) {
         TransactionFactory factory = createTransactionFactory(i);
         producers.add(i, new ClosedProducer(this, producerRates[i], i, parameters, factory));
         //(this, producerRates[i], i));
      }
      return producers;
   }

   /**
    * Class in charge of create or update the Producer in base of arrival rate. So for the open system
    */
   public synchronized List<Producer> createProducers(OpenSystem system) {

      log.info("Creating/Updating producers");

      ProducerRate[] producerRates;
      if (cacheWrapper.isPassiveReplication()) {
         if (cacheWrapper.isTheMaster()) {
            log.info("Creating producers groups for the master. Write transaction percentage is " + getWriteWeight());
            producerRates = new GroupProducerRateFactory(system.getWorkloadGenerator().getRateDistribution(),
                                                         getWriteWeight(),
                                                         1,
                                                         parameters.getNodeIndex(),
                                                         AbstractBenchmarkStressor.AVERAGE_PRODUCER_SLEEP_TIME).create();
         } else {
            log.info("Creating producers groups for the slave. Read-only transaction percentage is " + getReadWeight());
            producerRates = new GroupProducerRateFactory(system.getWorkloadGenerator().getRateDistribution(),
                                                         getReadWeight(),
                                                         cacheWrapper.getNumMembers() - 1,
                                                         parameters.getNodeIndex() == 0 ? parameters.getNodeIndex() : parameters.getNodeIndex() - 1,
                                                         AbstractBenchmarkStressor.AVERAGE_PRODUCER_SLEEP_TIME).create();
         }
      } else {
         log.info("Creating producers groups");
         producerRates = new GroupProducerRateFactory(system.getWorkloadGenerator().getRateDistribution(),
                                                      system.getWorkloadGenerator().arrivalRate(),
                                                      cacheWrapper.getNumMembers(),
                                                      parameters.getNodeIndex(),
                                                      AbstractBenchmarkStressor.AVERAGE_PRODUCER_SLEEP_TIME).create();

      }

      List<Producer> producers = new ArrayList<Producer>();
      for (int i = 0; i < producerRates.length; ++i) {
         TransactionFactory factory = createTransactionFactory(i);
         Producer newProducer = new OpenProducer(this, producerRates[i], i, parameters, factory);
         producers.add(i, newProducer);
         log.info("Create new producer with lamba: " + producerRates[i].getLambda());
      }

      if(producerRates.length != producers.size()){
         throw new RuntimeException("producerRates size != producers size");

      } else {
         log.info("Running with " + producers.size() + " producers");
      }

      return producers;
   }


   //public abstract StressorStats createStatsContainer();

   protected final Map<String, String> processResults(List<S> consumers) {

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
                     ". Test duration is: " + Utils.getMillisDurationString(java.lang.System.currentTimeMillis() - startTime));
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
      synchronized (running) {
         if (running.get()) {

            // potrebbe essere cambiato l'arrival rate e/o la dimensione del cluster
            // aggiorno i producer

            Integer cmd = (Integer) arg;

            switch (cmd) {
               case AbstractWorkloadGenerator.ARRIVAL_RATE_CHANGED:

                  if (!system.getType().equals(System.SystemType.OPEN))
                     throw new IllegalStateException("Arrival rate changed on a not Open system!!");
                  else
                     log.info("Arrival rate changed:" + ((OpenSystem) system).getWorkloadGenerator().arrivalRate());

                  if (((OpenSystem) system).getWorkloadGenerator().arrivalRate() != this.lastArrivalRate) {
                     this.lastArrivalRate = ((OpenSystem) system).getWorkloadGenerator().arrivalRate();
                     stopCreateStartProducers((OpenSystem) system);
                  }
                  break;
               default:
                  log.warn("Unrecognized argument: " + cmd);
                  break;
            }
         }
      }
   }

   public final synchronized int getNumberOfActiveThreads() {
      int count = 0;
      for (Consumer consumer : consumers) {
         if (consumer.isEnabled()) {
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
      Iterator<S> iterator = consumers.iterator();
      while (numOfThreads > 0 && iterator.hasNext()) {
         Consumer consumer = iterator.next();
         if (!consumer.isEnabled()) {
            consumer.enable();
         }
         numOfThreads--;
      }

      if (numOfThreads > 0) {
         int threadIdx = consumers.size();
         while (numOfThreads-- > 0) {
            S consumer = createConsumer(threadIdx++);
            consumerExecutorService.execute(consumer);
            consumers.add(consumer);
         }
      } else {
         while (iterator.hasNext()) {
            iterator.next().disable();
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

   public void changeNumberNodes() {

      try {

         log.warn("************************************************************");
         log.info("VIEW has changed: #slaves = " + cacheWrapper.getNumMembers());
         log.info("Slave info: cacheSize " + cacheWrapper.getCacheSize());
         log.warn("************************************************************");

         if (system instanceof IProducerSystem) {
            stopCreateStartProducers((IProducerSystem) system);
         } else {
            log.info("Ignoring view changed because working on Mule System");
         }

      } catch (Exception e) {
         log.info("WARNING! An exception has been masked. Usually the exception is thrown when more slaves have been stopped at the same time, in this case you can be happy, otherwise check it out!");
         log.info(e, e);
      }


   }


   /**
    * Returns the queue size
    *
    * @return queue size if queue not null, otherwise -2
    */
   public int queueSize() {
      if (queue != null) {
         return queue.size();
      } else {
         return -2;
      }
   }



    /* ******************** */
    /* *** GETTER/SETTER ** */
    /* ******************** */


}
