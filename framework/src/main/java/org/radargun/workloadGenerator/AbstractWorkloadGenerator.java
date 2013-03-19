package org.radargun.workloadGenerator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.DistStage;

import java.io.Serializable;
import java.util.Observable;

/**
 * Created with IntelliJ IDEA.
 * User: Fabio Perfetti
 * Date: 3/18/13
 * Time: 5:10 PM
 *
 */
public abstract class AbstractWorkloadGenerator extends Observable implements Cloneable, Runnable, Serializable {

    private static Log log = LogFactory.getLog(AbstractWorkloadGenerator.class);

    /**
     * in seconds
     */
    private double granularity = 1.0;
    public void setGranularity(double granularity) { this.granularity = granularity; }
    public double getGranularity() { return this.granularity; }

    /**
     * init time
     */
    private double initTime = 0.0;
    public void setInitTime(double initTime) { this.initTime = initTime; }
    public double getInitTime() { return this.initTime; }

    /**
     * current time
     */
    private double t = 0.0;
    public double getTime(){ return t; }

    /**
     * in seconds
     */
    private int maxArrivalRate = -1;
    public void setMaxArrivalRate(int maxArrivalRate) { this.maxArrivalRate = maxArrivalRate; }
    public int getMaxArrivalRate() { return this.maxArrivalRate; }

    private volatile boolean running = true;

    @Override
    public void run() {
        t = initTime - granularity;
        running = true;
        do{
            t+=granularity;
            setChanged();
            notifyObservers();
            try {
                Thread.sleep((long) granularity * 1000);
            } catch (InterruptedException e) {
                log.warn("Workload generator stopped in a non proper way. Please use stop() method");
                running = false;
            }
        } while (running);
    }

    public abstract int getCurrentArrivalRate();

    public boolean isOpenSystem(){ return true; }

    public void stop() { running = false; }

    public AbstractWorkloadGenerator clone() {
        try {
            return (AbstractWorkloadGenerator) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new IllegalStateException(e);
        }
    }

}