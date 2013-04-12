package org.radargun.stressors.tpcc;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.CacheWrapper;
import org.radargun.Transaction;
import org.radargun.portings.tpcc.ElementNotFoundException;
import org.radargun.portings.tpcc.transaction.NewOrderTransaction;
import org.radargun.portings.tpcc.transaction.PaymentTransaction;
import org.radargun.producer.ProducerRate;
import org.radargun.stressors.AbstractBenchmarkStressor;
import org.radargun.portings.tpcc.TpccTerminal;
import org.radargun.portings.tpcc.TpccTools;
import org.radargun.stressors.tpcc.TpccStats;
import org.radargun.utils.Utils;
import org.radargun.workloadGenerator.AbstractWorkloadGenerator;


import java.util.*;
import java.util.concurrent.CountDownLatch;


/**
 * On multiple threads executes implementations of TPC-C Transaction Profiles against the CacheWrapper, and returns the
 * result as a Map.
 *
 * @author peluso@gsd.inesc-id.pt , peluso@dis.uniroma1.it
 * @author Pedro Ruivo
 * @author Fabio Perfetti (refactored)
 */

public class TpccStressor extends AbstractBenchmarkStressor<TpccStressor.TpccConsumer> {


    private static Log log = LogFactory.getLog(TpccStressor.class);

    /**
     * percentage of Payment transactions
     */
    private int paymentWeight = 45;

    /**
     * percentage of Order Status transactions
     */
    private int orderStatusWeight = 5;

    /**
     * if true, each node will pick a warehouse and all transactions will work over that warehouse. The warehouses are
     * picked by order, i.e., slave 0 gets warehouse 1,N+1, 2N+1,[...]; ... slave N-1 gets warehouse N, 2N, [...].
     */
    private boolean accessSameWarehouse = false;

    /**
     * specify the min and the max number of items created by a New Order Transaction.
     * format: min,max
     */
    private String numberOfItemsInterval = null;

    private final List<Integer> listLocalWarehouses = new LinkedList<Integer>();


    /* ******************* */
    /* *** CONSTRUCTOR *** */
    /* ******************* */

    public TpccStressor(AbstractWorkloadGenerator loadGenerator) {
        super(loadGenerator);
    }

    /* ****************** */
    /* *** OVERRIDING *** */
    /* ****************** */

    @Override
    protected void initialization(){
        updateNumberOfItemsInterval();
        initializeToolsParameters();
        calculateLocalWarehouses();
    }

    @Override
    protected RequestType nextTransaction() {
        TpccTerminal terminal = new TpccTerminal(paymentWeight, orderStatusWeight, nodeIndex, 0);
        return new RequestType( System.nanoTime(), terminal.chooseTransactionType(
                                                                                  cacheWrapper.isPassiveReplication(),
                                                                                  cacheWrapper.isTheMaster()
                                                                                ) );
    }

    @Override
    protected Transaction generateTransaction(RequestType type, int threadIndex) {

        TpccConsumer consumer = this.consumers.get(threadIndex);
        Transaction transaction = consumer.getTerminal().createTransaction(type.transactionType, threadIndex);
        return transaction;
    }

    @Override
    public Transaction choiceTransaction(boolean isPassiveReplication, boolean isTheMaster, int threadId) {
        TpccConsumer consumer = this.consumers.get(threadId);
        Transaction transaction = consumer.getTerminal().choiceTransaction(cacheWrapper.isPassiveReplication(), cacheWrapper.isTheMaster(), threadId);
        log.info("Closed system: starting a brand new transaction of type " + transaction.getType());

        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    protected TpccConsumer createConsumer(int threadIndex) {
        int localWarehouse = getWarehouseForThread(threadIndex);
        return new TpccConsumer(localWarehouse, threadIndex, nodeIndex, paymentWeight, orderStatusWeight);
    }

    @Override
    protected void validateTransactionsWeight() {
        int sum = orderStatusWeight + paymentWeight;
        if (sum < 0 || sum > 100) {
            throw new IllegalArgumentException("The sum of the transactions weights must be higher or equals than zero " +
                    "and less or equals than one hundred");
        }
    }

    @Override
    protected double getWriteWeight() {
        double writeWeight = Math.max(100 - orderStatusWeight, paymentWeight) / 100D;
        return writeWeight;
    }

    @Override
    protected double getReadWeight() {
        double readWeight = orderStatusWeight / 100D;
        return readWeight;
    }

    @Override
    protected Map<String, String> processResults(List<TpccConsumer> consumers) {

        long duration = 0;

        int reads = 0;
        int writes = 0;
        int newOrderTransactions = 0;
        int paymentTransactions = 0;

        int failures = 0;
        int rdFailures = 0;
        int wrFailures = 0;
        int nrWrFailuresOnCommit = 0;
        int newOrderFailures = 0;
        int paymentFailures = 0;
        int appFailures = 0;

        long readsDurations = 0L;
        long writesDurations = 0L;
        long newOrderDurations = 0L;
        long paymentDurations = 0L;
        long successful_writesDurations = 0L;
        long successful_readsDurations = 0L;
        long writeServiceTimes = 0L;
        long readServiceTimes = 0L;
        long newOrderServiceTimes = 0L;
        long paymentServiceTimes = 0L;

        long successful_commitWriteDurations = 0L;
        long aborted_commitWriteDurations = 0L;
        long commitWriteDurations = 0L;

        long writeInQueueTimes = 0L;
        long readInQueueTimes = 0L;
        long newOrderInQueueTimes = 0L;
        long paymentInQueueTimes = 0L;
        long numWritesDequeued = 0L;
        long numReadsDequeued = 0L;
        long numNewOrderDequeued = 0L;
        long numPaymentDequeued = 0L;
        long numLocalTimeout = 0L;
        long numRemoteTimeout = 0L;

        double backOffTime = 0D;
        double backOffs = 0D;

        for (TpccConsumer tpccConsumer : consumers) {

            TpccStats tpccStats = tpccConsumer.stats;

            //duration += stressor.totalDuration(); //in nanosec
            //readsDurations += stressor.readDuration; //in nanosec
            //writesDurations += stressor.writeDuration; //in nanosec
            newOrderDurations += tpccStats.getNewOrderDuration(); //in nanosec
            paymentDurations += tpccStats.getPaymentDuration(); //in nanosec
            successful_writesDurations += tpccStats.getSuccessfulWriteDuration(); //in nanosec
            successful_readsDurations += tpccStats.getSuccessfulReadDuration(); //in nanosec

            successful_commitWriteDurations += tpccStats.getSuccessfulCommitWriteDuration(); //in nanosec
            aborted_commitWriteDurations += tpccStats.getAbortedCommitWriteDuration(); //in nanosec
            commitWriteDurations += tpccStats.getCommitWriteDuration(); //in nanosec;

            writeServiceTimes += tpccStats.getWriteServiceTime();
            readServiceTimes += tpccStats.getReadServiceTime();
            newOrderServiceTimes += tpccStats.getNewOrderServiceTime();
            paymentServiceTimes += tpccStats.getPaymentServiceTime();

            reads += tpccStats.getReads();
            writes += tpccStats.getWrites();
            newOrderTransactions += tpccStats.getNewOrder();
            paymentTransactions += tpccStats.getPayment();

            failures += tpccStats.getNrFailures();
            rdFailures += tpccStats.getNrRdFailures();
            wrFailures += tpccStats.getNrWrFailures();
            nrWrFailuresOnCommit += tpccStats.getNrWrFailuresOnCommit();
            newOrderFailures += tpccStats.getNrNewOrderFailures();
            paymentFailures += tpccStats.getNrPaymentFailures();
            appFailures += tpccStats.getAppFailures();

            writeInQueueTimes += tpccStats.getWriteInQueueTime();
            readInQueueTimes += tpccStats.getReadInQueueTime();
            newOrderInQueueTimes += tpccStats.getNewOrderInQueueTime();
            paymentInQueueTimes += tpccStats.getPaymentInQueueTime();
            numWritesDequeued += tpccStats.getNumWriteDequeued();
            numReadsDequeued += tpccStats.getNumReadDequeued();
            numNewOrderDequeued += tpccStats.getNumNewOrderDequeued();
            numPaymentDequeued += tpccStats.getNumPaymentDequeued();
            numLocalTimeout += tpccStats.getLocalTimeout();
            numRemoteTimeout += tpccStats.getRemoteTimeout();
            backOffs += tpccStats.getNumBackOffs();
            backOffTime += tpccStats.getBackedOffTime();
        }

        //duration = duration / 1000000; // nanosec to millisec
        //readsDurations = readsDurations / 1000; //nanosec to microsec
        //writesDurations = writesDurations / 1000; //nanosec to microsec
        //newOrderDurations = newOrderDurations / 1000; //nanosec to microsec
        //paymentDurations = paymentDurations / 1000;//nanosec to microsec
        successful_readsDurations = successful_readsDurations / 1000; //nanosec to microsec
        successful_writesDurations = successful_writesDurations / 1000; //nanosec to microsec
        successful_commitWriteDurations = successful_commitWriteDurations / 1000; //nanosec to microsec
        aborted_commitWriteDurations = aborted_commitWriteDurations / 1000; //nanosec to microsec
        commitWriteDurations = commitWriteDurations / 1000; //nanosec to microsec
        writeServiceTimes = writeServiceTimes / 1000; //nanosec to microsec
        readServiceTimes = readServiceTimes / 1000; //nanosec to microsec
        newOrderServiceTimes = newOrderServiceTimes / 1000; //nanosec to microsec
        paymentServiceTimes = paymentServiceTimes / 1000; //nanosec to microsec

        writeInQueueTimes = writeInQueueTimes / 1000;//nanosec to microsec
        readInQueueTimes = readInQueueTimes / 1000;//nanosec to microsec
        newOrderInQueueTimes = newOrderInQueueTimes / 1000;//nanosec to microsec
        paymentInQueueTimes = paymentInQueueTimes / 1000;//nanosec to microsec

        Map<String, String> results = new LinkedHashMap<String, String>();

        duration = endTime - startTime;

        results.put("STOPPED", str(this.stoppedByJmx));

        results.put("DURATION (msec)", str(duration));
        double requestPerSec = (reads + writes) / (duration / 1000.0);
        results.put("REQ_PER_SEC", str(requestPerSec));

        double wrtPerSec = 0;
        double rdPerSec = 0;
        double newOrderPerSec = 0;
        double paymentPerSec = 0;
        double cpu = 0, mem = 0;
        if (duration == 0)
            results.put("READS_PER_SEC", str(0));
        else {
            rdPerSec = reads / (duration / 1000.0);
            results.put("READS_PER_SEC", str(rdPerSec));
        }

        if (duration == 0)
            results.put("WRITES_PER_SEC", str(0));
        else {
            wrtPerSec = writes / (duration / 1000.0);
            results.put("WRITES_PER_SEC", str(wrtPerSec));
        }

        if (duration == 0)
            results.put("NEW_ORDER_PER_SEC", str(0));
        else {
            newOrderPerSec = newOrderTransactions / (duration / 1000.0);
            results.put("NEW_ORDER_PER_SEC", str(newOrderPerSec));
        }
        if (duration == 0)
            results.put("PAYMENT_PER_SEC", str(0));
        else {
            paymentPerSec = paymentTransactions / (duration / 1000.0);
            results.put("PAYMENT_PER_SEC", str(paymentPerSec));
        }

        results.put("READ_COUNT", str(reads));
        results.put("WRITE_COUNT", str(writes));
        results.put("NEW_ORDER_COUNT", str(newOrderTransactions));
        results.put("PAYMENT_COUNT", str(paymentTransactions));
        results.put("FAILURES", str(failures));
        results.put("APPLICATION_FAILURES", str(appFailures));
        results.put("WRITE_FAILURES", str(wrFailures));
        results.put("NEW_ORDER_FAILURES", str(newOrderFailures));
        results.put("PAYMENT_FAILURES", str(paymentFailures));
        results.put("READ_FAILURES", str(rdFailures));

        if ((reads + writes) != 0)
            results.put("AVG_SUCCESSFUL_DURATION (usec)", str((successful_writesDurations + successful_readsDurations) / (reads + writes)));
        else
            results.put("AVG_SUCCESSFUL_DURATION (usec)", str(0));


        if (reads != 0)
            results.put("AVG_SUCCESSFUL_READ_DURATION (usec)", str(successful_readsDurations / reads));
        else
            results.put("AVG_SUCCESSFUL_READ_DURATION (usec)", str(0));


        if (writes != 0)
            results.put("AVG_SUCCESSFUL_WRITE_DURATION (usec)", str(successful_writesDurations / writes));
        else
            results.put("AVG_SUCCESSFUL_WRITE_DURATION (usec)", str(0));


        if (writes != 0) {
            results.put("AVG_SUCCESSFUL_COMMIT_WRITE_DURATION (usec)", str((successful_commitWriteDurations / writes)));
        } else {
            results.put("AVG_SUCCESSFUL_COMMIT_WRITE_DURATION (usec)", str(0));
        }

        if (nrWrFailuresOnCommit != 0) {
            results.put("AVG_ABORTED_COMMIT_WRITE_DURATION (usec)", str((aborted_commitWriteDurations / nrWrFailuresOnCommit)));
        } else {
            results.put("AVG_ABORTED_COMMIT_WRITE_DURATION (usec)", str(0));
        }


        if (writes + nrWrFailuresOnCommit != 0) {
            results.put("AVG_COMMIT_WRITE_DURATION (usec)", str((commitWriteDurations / (writes + nrWrFailuresOnCommit))));
        } else {
            results.put("AVG_COMMIT_WRITE_DURATION (usec)", str(0));
        }

        if ((reads + rdFailures) != 0)
            results.put("AVG_RD_SERVICE_TIME (usec)", str(readServiceTimes / (reads + rdFailures)));
        else
            results.put("AVG_RD_SERVICE_TIME (usec)", str(0));

        if ((writes + wrFailures) != 0)
            results.put("AVG_WR_SERVICE_TIME (usec)", str(writeServiceTimes / (writes + wrFailures)));
        else
            results.put("AVG_WR_SERVICE_TIME (usec)", str(0));

        if ((newOrderTransactions + newOrderFailures) != 0)
            results.put("AVG_NEW_ORDER_SERVICE_TIME (usec)", str(newOrderServiceTimes / (newOrderTransactions + newOrderFailures)));
        else
            results.put("AVG_NEW_ORDER_SERVICE_TIME (usec)", str(0));

        if ((paymentTransactions + paymentFailures) != 0)
            results.put("AVG_PAYMENT_SERVICE_TIME (usec)", str(paymentServiceTimes / (paymentTransactions + paymentFailures)));
        else
            results.put("AVG_PAYMENT_SERVICE_TIME (usec)", str(0));

        if (numWritesDequeued != 0)
            results.put("AVG_WR_INQUEUE_TIME (usec)", str(writeInQueueTimes / numWritesDequeued));
        else
            results.put("AVG_WR_INQUEUE_TIME (usec)", str(0));
        if (numReadsDequeued != 0)
            results.put("AVG_RD_INQUEUE_TIME (usec)", str(readInQueueTimes / numReadsDequeued));
        else
            results.put("AVG_RD_INQUEUE_TIME (usec)", str(0));
        if (numNewOrderDequeued != 0)
            results.put("AVG_NEW_ORDER_INQUEUE_TIME (usec)", str(newOrderInQueueTimes / numNewOrderDequeued));
        else
            results.put("AVG_NEW_ORDER_INQUEUE_TIME (usec)", str(0));
        if (numPaymentDequeued != 0)
            results.put("AVG_PAYMENT_INQUEUE_TIME (usec)", str(paymentInQueueTimes / numPaymentDequeued));
        else
            results.put("AVG_PAYMENT_INQUEUE_TIME (usec)", str(0));
        if (numLocalTimeout != 0)
            results.put("LOCAL_TIMEOUT", str(numLocalTimeout));
        else
            results.put("LOCAL_TIMEOUT", str(0));
        if (numRemoteTimeout != 0)
            results.put("REMOTE_TIMEOUT", str(numRemoteTimeout));
        else
            results.put("REMOTE_TIMEOUT", str(0));
        if (backOffs != 0)
            results.put("AVG_BACKOFF", str(backOffTime / backOffs));
        else
            results.put("AVG_BACKOFF", str(0));

        results.put("NumThreads", str(numOfThreads));

        if (statSampler != null) {
            cpu = statSampler.getAvgCpuUsage();
            mem = statSampler.getAvgMemUsage();
        }
        results.put("CPU_USAGE", str(cpu));
        results.put("MEMORY_USAGE", str(mem));
        results.putAll(cacheWrapper.getAdditionalStats());
        results.put("TEST_ID", this.testIdString(paymentWeight, orderStatusWeight, numOfThreads));
        saveSamples();

        log.info("Sending map to master " + results.toString());

        log.info("Finished generating report. Nr of failed operations on this node is: " + failures +
                ". Test duration is: " + Utils.getMillisDurationString(System.currentTimeMillis() - startTime));
        return results;
    }



    /* *************** */
    /* *** METHODS *** */
    /* *************** */

    private void updateNumberOfItemsInterval() {
        if (numberOfItemsInterval == null) {
            return;
        }
        String[] split = numberOfItemsInterval.split(",");

        if (split.length != 2) {
            log.info("Cannot update the min and max values for the number of items in new order transactions. " +
                    "Using the default values");
            return;
        }

        try {
            TpccTools.NUMBER_OF_ITEMS_INTERVAL[0] = Integer.parseInt(split[0]);
        } catch (NumberFormatException nfe) {
            log.warn("Min value is not a number. " + nfe.getLocalizedMessage());
        }

        try {
            TpccTools.NUMBER_OF_ITEMS_INTERVAL[1] = Integer.parseInt(split[1]);
        } catch (NumberFormatException nfe) {
            log.warn("Max value is not a number. " + nfe.getLocalizedMessage());
        }
    }

    private void initializeToolsParameters() {
        try {
            TpccTools.C_C_LAST = (Long) cacheWrapper.get(null, "C_C_LAST");

            TpccTools.C_C_ID = (Long) cacheWrapper.get(null, "C_C_ID");

            TpccTools.C_OL_I_ID = (Long) cacheWrapper.get(null, "C_OL_ID");

        } catch (Exception e) {
            log.error("Error", e);
        }
    }

    private String str(Object o) {
        return String.valueOf(o);
    }




    /* ********************* */
    /* *** GETTER/SETTER *** */
    /* ********************* */

    public void setPaymentWeight(int paymentWeight) {
        this.paymentWeight = paymentWeight;
    }

    public void setOrderStatusWeight(int orderStatusWeight) {
        this.orderStatusWeight = orderStatusWeight;
    }

    public void setAccessSameWarehouse(boolean accessSameWarehouse) {
        this.accessSameWarehouse = accessSameWarehouse;
    }

    public void setNumberOfItemsInterval(String numberOfItemsInterval) {
        this.numberOfItemsInterval = numberOfItemsInterval;
    }



    @Override
    public String toString() {
        return "TpccStressor{" +
                "updateTimes=" + perThreadSimulTime +
                ", paymentWeight=" + paymentWeight +
                ", orderStatusWeight=" + orderStatusWeight +
                ", accessSameWarehouse=" + accessSameWarehouse +
                ", numSlaves=" + numSlaves +
                ", nodeIndex=" + nodeIndex +
                ", numOfThreads=" + numOfThreads +
                ", numberOfItemsInterval=" + numberOfItemsInterval +
                ", statsSamplingInterval=" + statsSamplingInterval +
                '}';
    }


    private void calculateLocalWarehouses() {
        if (accessSameWarehouse) {
            TpccTools.selectLocalWarehouse(numSlaves, nodeIndex, listLocalWarehouses);
            if (log.isDebugEnabled()) {
                log.debug("Find the local warehouses. Number of Warehouses=" + TpccTools.NB_WAREHOUSES + ", number of slaves=" +
                        numSlaves + ", node index=" + nodeIndex + ".Local warehouses are " + listLocalWarehouses);
            }
        } else {
            if (log.isDebugEnabled()) {
                log.debug("Local warehouses are disabled. Choose a random warehouse in each transaction");
            }
        }
    }

   /*
    * For the review, the workload is the following:
    * 
    * high contention: change(1, 85, 10);
    * low contention: change(nodeIndex + 1, 45, 50); 
    */

//    public synchronized final void highContention(int payment, int order) {
//        if (!running.get()) {
//            return;
//        }
//        paymentWeight = payment;
//        orderStatusWeight = order;
//
//        log.info("Change to high contention workload:");
//        for (Stressor stressor : consumers) {
//            stressor.terminal.change(1, paymentWeight, orderStatusWeight);
//            log.info(stressor.getName() + " terminal is " + stressor.terminal);
//        }
//        for (Producer producer : producers) {
//            producer.terminal.change(1, paymentWeight, orderStatusWeight);
//            log.info(producer.getName() + " terminal is " + producer.terminal);
//        }
//    }

//    public synchronized final void lowContention(int payment, int order) {
//        if (!running.get()) {
//            return;
//        }
//        if (listLocalWarehouses.isEmpty()) {
//            TpccTools.selectLocalWarehouse(numSlaves, nodeIndex, listLocalWarehouses);
//        }
//        paymentWeight = payment;
//        orderStatusWeight = order;
//
//        log.info("Change to low contention workload:");
//        for (Stressor stressor : consumers) {
//            stressor.terminal.change(getWarehouseForThread(stressor.threadIndex), paymentWeight, orderStatusWeight);
//            log.info(stressor.getName() + " terminal is " + stressor.terminal);
//        }
//        for (Producer producer : producers) {
//            //in the producers, the warehouse is not needed
//            producer.terminal.change(-1, paymentWeight, orderStatusWeight);
//            log.info(producer.getName() + " terminal is " + producer.terminal);
//        }
//    }

//    public synchronized final void randomContention(int payment, int order) {
//        if (!running.get()) {
//            return;
//        }
//        paymentWeight = payment;
//        orderStatusWeight = order;
//
//        log.info("Change to random contention workload:");
//        for (Stressor stressor : consumers) {
//            stressor.terminal.change(-1, paymentWeight, orderStatusWeight);
//            log.info(stressor.getName() + " terminal is " + stressor.terminal);
//        }
//        for (Producer producer : producers) {
//            producer.terminal.change(-1, paymentWeight, orderStatusWeight);
//            log.info(producer.getName() + " terminal is " + producer.terminal);
//        }
//    }

    private int getWarehouseForThread(int threadIdx) {
        return listLocalWarehouses.isEmpty() ? -1 : listLocalWarehouses.get(threadIdx % listLocalWarehouses.size());
    }

    public synchronized final double getExpectedWritePercentage() {
        return 1.0 - (orderStatusWeight / 100.0);
    }

    public synchronized final int getPaymentWeight() {
        return paymentWeight;
    }

    public synchronized final int getOrderStatusWeight() {
        return orderStatusWeight;
    }



    private String testIdString(long payment, long orderStatus, long threads) {
        return threads + "T_" + payment + "PA_" + orderStatus + "OS";
    }


    /* ************************** */
    /* *** TpccConsumer CLASS *** */
    /* ************************** */

    protected class TpccConsumer extends Consumer<TpccStats> {

        private final TpccTerminal terminal;

        public TpccConsumer(int localWarehouseID, int threadIndex, int nodeIndex, double paymentWeight, double orderStatusWeight) {
            super(threadIndex);

            stats = new TpccStats();
            this.threadIndex = threadIndex;
            this.terminal = new TpccTerminal(paymentWeight, orderStatusWeight, nodeIndex, localWarehouseID);
        }

        public TpccTerminal getTerminal(){ return terminal; }

    }


   /* ************************** */
   /* ***** PRODUCER CLASS ***** */
   /* ************************** */

//    private class Producer extends Thread {
//        private final ProducerRate rate;
//        private final TpccTerminal terminal;
//        private boolean running = false;
//
//        public Producer(ProducerRate rate, int id) {
//            super("Producer-" + id);
//            setDaemon(true);
//            this.rate = rate;
//            this.terminal = new TpccTerminal(paymentWeight, orderStatusWeight, nodeIndex, 0);
//        }
//
//        public void run() {
//            if (log.isDebugEnabled()) {
//                log.debug("Starting " + getName() + " with rate of " + rate.getLambda());
//            }
//            while (assertRunning()) {
//                queue.offer(new RequestType(System.nanoTime(), terminal.chooseTransactionType(
//                        cacheWrapper.isPassiveReplication(), cacheWrapper.isTheMaster()
//                )));
//                countJobs.incrementAndGet();
//                rate.sleep();
//            }
//        }
//
//        private synchronized boolean assertRunning() {
//            return running;
//        }
//
//        @Override
//        public synchronized void start() {
//            if (running) return;
//            running = true;
//            super.start();
//        }
//
//        @Override
//        public synchronized void interrupt() {
//            if (!running) return;
//            running = false;
//            super.interrupt();
//        }
//    }

}
