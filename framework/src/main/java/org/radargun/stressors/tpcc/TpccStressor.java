package org.radargun.stressors.tpcc;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.Transaction;
import org.radargun.producer.ProducerRate;
import org.radargun.stressors.AbstractBenchmarkStressor;
import org.radargun.portings.tpcc.TpccTerminal;
import org.radargun.portings.tpcc.TpccTools;
import org.radargun.stressors.commons.StressorStats;
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
 */

public class TpccStressor extends AbstractBenchmarkStressor {


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


    public TpccStressor(AbstractWorkloadGenerator loadGenerator) {
        super(loadGenerator);
    }


    @Override
    protected void initialization(){
        updateNumberOfItemsInterval();
        initializeToolsParameters();
    }

    @Override
    protected RequestType nextTransaction() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    protected Transaction generateTransaction(RequestType request, int threadIndex, StressorStats stats) {

        long endInQueueTime = System.nanoTime();

        TpccStats tpccStats = (TpccStats) stats;
        if (request.transactionType == TpccTerminal.NEW_ORDER) {
            tpccStats.numWriteDequeued++;
            tpccStats.numNewOrderDequeued++;
            tpccStats.writeInQueueTime += endInQueueTime - request.timestamp;
            tpccStats.newOrderInQueueTime += endInQueueTime - request.timestamp;
        } else if (request.transactionType == TpccTerminal.PAYMENT) {
            tpccStats.numWriteDequeued++;
            tpccStats.numPaymentDequeued++;
            tpccStats.writeInQueueTime += endInQueueTime - request.timestamp;
            tpccStats.paymentInQueueTime += endInQueueTime - request.timestamp;
        } else if (request.transactionType == TpccTerminal.ORDER_STATUS) {
            tpccStats.numReadDequeued++;
            tpccStats.readInQueueTime += endInQueueTime - request.timestamp;
        }


        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Transaction choiceTransaction(boolean isPassiveReplication, boolean isTheMaster, int threadId) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }


    public void destroy() throws Exception {
        log.warn("Attention: going to destroy the wrapper");
        cacheWrapper.empty();
        cacheWrapper = null;
    }

    @Override
    protected void validateTransactionsWeight() {
        int sum = orderStatusWeight + paymentWeight;
        if (sum < 0 || sum > 100) {
            throw new IllegalArgumentException("The sum of the transactions weights must be higher or equals than zero " +
                    "and less or equals than one hundred");
        }
    }

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

    @Override
    protected Map<String, String> processResults(List<Consumer> stressors) {

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

        for (Stressor stressor : stressors) {
            //duration += stressor.totalDuration(); //in nanosec
            //readsDurations += stressor.readDuration; //in nanosec
            //writesDurations += stressor.writeDuration; //in nanosec
            newOrderDurations += stressor.newOrderDuration; //in nanosec
            paymentDurations += stressor.paymentDuration; //in nanosec
            successful_writesDurations += stressor.successful_writeDuration; //in nanosec
            successful_readsDurations += stressor.successful_readDuration; //in nanosec

            successful_commitWriteDurations += stressor.successful_commitWriteDuration; //in nanosec
            aborted_commitWriteDurations += stressor.aborted_commitWriteDuration; //in nanosec
            commitWriteDurations += stressor.commitWriteDuration; //in nanosec;

            writeServiceTimes += stressor.writeServiceTime;
            readServiceTimes += stressor.readServiceTime;
            newOrderServiceTimes += stressor.newOrderServiceTime;
            paymentServiceTimes += stressor.paymentServiceTime;

            reads += stressor.reads;
            writes += stressor.writes;
            newOrderTransactions += stressor.newOrder;
            paymentTransactions += stressor.payment;

            failures += stressor.nrFailures;
            rdFailures += stressor.nrRdFailures;
            wrFailures += stressor.nrWrFailures;
            nrWrFailuresOnCommit += stressor.nrWrFailuresOnCommit;
            newOrderFailures += stressor.nrNewOrderFailures;
            paymentFailures += stressor.nrPaymentFailures;
            appFailures += stressor.appFailures;

            writeInQueueTimes += stressor.writeInQueueTime;
            readInQueueTimes += stressor.readInQueueTime;
            newOrderInQueueTimes += stressor.newOrderInQueueTime;
            paymentInQueueTimes += stressor.paymentInQueueTime;
            numWritesDequeued += stressor.numWriteDequeued;
            numReadsDequeued += stressor.numReadDequeued;
            numNewOrderDequeued += stressor.numNewOrderDequeued;
            numPaymentDequeued += stressor.numPaymentDequeued;
            numLocalTimeout += stressor.localTimeout;
            numRemoteTimeout += stressor.remoteTimeout;
            backOffs += stressor.numBackOffs;
            backOffTime += stressor.backedOffTime;
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

    protected List<Consumer> executeOperations() throws Exception {
        calculateLocalWarehouses();

        startPoint = new CountDownLatch(1);
        for (int threadIndex = 0; threadIndex < numOfThreads; threadIndex++) {
            Consumer consumer = createStressor(threadIndex);
            consumers.add(consumer);
            consumer.start();
        }
        log.info("Cache wrapper info is: " + cacheWrapper.getInfo());
        startPoint.countDown();
        blockWhileRunning();
        for (Consumer consumer : consumers) {
            consumer.join();
        }
        log.info("Fine ExecuteOperations");
        endTime = System.currentTimeMillis();
        return consumers;
    }



   /* ************************** */
   /* ***** PRODUCER CLASS ***** */
   /* ************************** */

    private class Producer extends Thread {
        private final ProducerRate rate;
        private final TpccTerminal terminal;
        private boolean running = false;

        public Producer(ProducerRate rate, int id) {
            super("Producer-" + id);
            setDaemon(true);
            this.rate = rate;
            this.terminal = new TpccTerminal(paymentWeight, orderStatusWeight, nodeIndex, 0);
        }

        public void run() {
            if (log.isDebugEnabled()) {
                log.debug("Starting " + getName() + " with rate of " + rate.getLambda());
            }
            while (assertRunning()) {
                queue.offer(new RequestType(System.nanoTime(), terminal.chooseTransactionType(
                        cacheWrapper.isPassiveReplication(), cacheWrapper.isTheMaster()
                )));
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


    private String str(Object o) {
        return String.valueOf(o);
    }

    public void setNumOfThreads(int numOfThreads) {
        this.numOfThreads = numOfThreads;
    }

    public void setNodeIndex(int nodeIndex) {
        this.nodeIndex = nodeIndex;
    }

    public void setNumSlaves(int value) {
        this.numSlaves = value;
    }

    public void setPerThreadSimulTime(long perThreadSimulTime) {
        this.perThreadSimulTime = perThreadSimulTime;
    }

    public void setRetryOnAbort(boolean retryOnAbort) {
        this.retryOnAbort = retryOnAbort;
    }

    public void setRetrySameXact(boolean b) {
        this.retrySameXact = b;
    }

    public void setBackOffTime(long backOffTime) {
        this.backOffTime = backOffTime;

    }

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

    public void setStatsSamplingInterval(long statsSamplingInterval) {
        this.statsSamplingInterval = statsSamplingInterval;
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





    public final synchronized void setNumberOfRunningThreads(int numOfThreads) {
        if (numOfThreads < 1 || !running.get()) {
            return;
        }
        Iterator<Stressor> iterator = consumers.iterator();
        while (numOfThreads > 0 && iterator.hasNext()) {
            Stressor stressor = iterator.next();
            if (!stressor.isActive()) {
                stressor.active();
            }
            numOfThreads--;
        }

        if (numOfThreads > 0) {
            int threadIdx = consumers.size();
            while (numOfThreads-- > 0) {
                Stressor stressor = createStressor(threadIdx++);
                stressor.start();
                consumers.add(stressor);
            }
        } else {
            while (iterator.hasNext()) {
                iterator.next().inactive();
            }
        }
    }

    public final synchronized int getNumberOfThreads() {
        return consumers.size();
    }

    public final synchronized int getNumberOfActiveThreads() {
        int count = 0;
        for (Stressor stressor : consumers) {
            if (stressor.isActive()) {
                count++;
            }
        }
        return count;
    }

    private Stressor createStressor(int threadIndex) {
        int localWarehouse = getWarehouseForThread(threadIndex);
        return new Stressor(localWarehouse, threadIndex, nodeIndex, paymentWeight, orderStatusWeight);
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

    public synchronized final void highContention(int payment, int order) {
        if (!running.get()) {
            return;
        }
        paymentWeight = payment;
        orderStatusWeight = order;

        log.info("Change to high contention workload:");
        for (Stressor stressor : consumers) {
            stressor.terminal.change(1, paymentWeight, orderStatusWeight);
            log.info(stressor.getName() + " terminal is " + stressor.terminal);
        }
        for (Producer producer : producers) {
            producer.terminal.change(1, paymentWeight, orderStatusWeight);
            log.info(producer.getName() + " terminal is " + producer.terminal);
        }
    }

    public synchronized final void lowContention(int payment, int order) {
        if (!running.get()) {
            return;
        }
        if (listLocalWarehouses.isEmpty()) {
            TpccTools.selectLocalWarehouse(numSlaves, nodeIndex, listLocalWarehouses);
        }
        paymentWeight = payment;
        orderStatusWeight = order;

        log.info("Change to low contention workload:");
        for (Stressor stressor : consumers) {
            stressor.terminal.change(getWarehouseForThread(stressor.threadIndex), paymentWeight, orderStatusWeight);
            log.info(stressor.getName() + " terminal is " + stressor.terminal);
        }
        for (Producer producer : producers) {
            //in the producers, the warehouse is not needed
            producer.terminal.change(-1, paymentWeight, orderStatusWeight);
            log.info(producer.getName() + " terminal is " + producer.terminal);
        }
    }

    public synchronized final void randomContention(int payment, int order) {
        if (!running.get()) {
            return;
        }
        paymentWeight = payment;
        orderStatusWeight = order;

        log.info("Change to random contention workload:");
        for (Stressor stressor : consumers) {
            stressor.terminal.change(-1, paymentWeight, orderStatusWeight);
            log.info(stressor.getName() + " terminal is " + stressor.terminal);
        }
        for (Producer producer : producers) {
            producer.terminal.change(-1, paymentWeight, orderStatusWeight);
            log.info(producer.getName() + " terminal is " + producer.terminal);
        }
    }

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

    public synchronized final void stopBenchmark() {
        this.stoppedByJmx = true;
        finishBenchmarkTimer.cancel();
        finishBenchmark();
    }

    private String testIdString(long payment, long orderStatus, long threads) {
        return threads + "T_" + payment + "PA_" + orderStatus + "OS";
    }

}
