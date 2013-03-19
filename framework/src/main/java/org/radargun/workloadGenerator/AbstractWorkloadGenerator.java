package org.radargun.workloadGenerator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.Serializable;
import java.util.Observable;
import java.util.concurrent.*;

/**
 * Created with IntelliJ IDEA.
 * User: Fabio Perfetti
 * Date: 3/18/13
 * Time: 5:10 PM
 *
 */
public abstract class AbstractWorkloadGenerator extends Observable implements Cloneable, Serializable {

    private static Log log = LogFactory.getLog(AbstractWorkloadGenerator.class);

    public enum RateDistribution {
        UNIFORM("UniformRate"), EXPONENTIAL("ExponentialRate");
        private String value;

        RateDistribution(String value) {
            this.value = value;
        }

        public String getDistributionRateName(){ return this.value; }
    }

    /**
     * default rate distribution: EXPONENTIAL
     */
    private RateDistribution rateDistribution = RateDistribution.EXPONENTIAL;
    public void setRateDistribution(String rateDistribution) { this.rateDistribution = RateDistribution.valueOf(rateDistribution.toUpperCase());  }
    public RateDistribution getRateDistribution() { return this.rateDistribution; }

    /**
     * granularity in milliseconds
     */
    private int granularity = 1000;
    public void setGranularity(int granularitySeconds) { this.granularity = granularitySeconds * 1000; }
    public int getGranularity() { return this.granularity; }

    /**
     * init time (in milliseconds)
     */
    private int initTime = 0;
    public void setInitTime(int initTime) { this.initTime = initTime *1000; }
    /**
     * returns init time (in second)
     */
    public double getInitTime() { return this.initTime/1000; }

    /**
     * current time (in millisecond)
     */
    private int t = 0;

    /**
     * returns current time in seconds
     * @return
     */
    public double getTime(){ return this.t/1000; }

    /**
     * in seconds
     */
    private int maxArrivalRate = -1;
    public void setMaxArrivalRate(int maxArrivalRate) { this.maxArrivalRate = maxArrivalRate; }
    public int getMaxArrivalRate() { return this.maxArrivalRate; }

    private volatile boolean running = true;

    public void start(){
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        executor.execute(new Starter());
        executor.shutdown();
    }

    public void stop(){
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        executor.execute(new Stopper());
        executor.shutdown();
    }

    /**
     * Returns the arrival rate.<br/>
     * If maxArrivalRate setted and getCurrentArrivalRate()>maxArrivalRate then maxArrivalRate will be returned
     * @return maxArrivalRate if getCurrentArrivalRate() > maxArrivalRate && maxArrivalRate > 0<br/>
     *  else getCurrentArrivalRate()
     */
    public int getArrivalRate(){
        if( getCurrentArrivalRate() < 0 ){
            return 0;
        }
        if( this.maxArrivalRate > 0 && this.getCurrentArrivalRate() > this.maxArrivalRate ){
            log.warn("MaxArrivalRate reached!");
            return getMaxArrivalRate();
        }
        return getCurrentArrivalRate();
    }

    protected abstract int getCurrentArrivalRate();

    public boolean isOpenSystem(){ return true; }

    public AbstractWorkloadGenerator clone() {
        try {
            return (AbstractWorkloadGenerator) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new IllegalStateException(e);
        }
    }



    /**** INNER CLASSES ****/

    private class Starter implements Runnable{
        @Override
        public void run() {
            BlockingQueue<Integer> queue = new LinkedBlockingQueue<Integer>();

            t = initTime - granularity;
            log.info("Scheduling timer");
            ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
            executor.scheduleWithFixedDelay(new TimeIncrementer(queue), granularity, granularity, TimeUnit.MILLISECONDS);
            while (running){
                log.info("waiting for producer");
                try {
                    queue.take();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                setChanged();
                notifyObservers();
            }
            executor.shutdown();
            try {
                executor.awaitTermination(2*granularity, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

        }
    }

    private class Stopper implements Runnable{
        @Override
        public void run() {
            log.info("Stopping workload generator");
            running = false;
        }
    }

    private class TimeIncrementer implements Runnable{

        BlockingQueue<Integer> queue;

        public TimeIncrementer(BlockingQueue<Integer> queue){ this.queue = queue; }

        @Override
        public void run() {
            log.info("Incrementing timer");
            t+=granularity;
            log.info("t=" + t);
            try {
                queue.put(t);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

}