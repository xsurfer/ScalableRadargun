package org.radargun.stages.stressors.producer;

import org.radargun.stages.stressors.AbstractBenchmarkStressor;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by: Fabio Perfetti
 * E-mail: perfabio87@gmail.com
 * Date: 4/18/13
 */
public class ClosedProducer extends Producer {

    private long thinkTime;
    private AtomicBoolean notified = new AtomicBoolean(false);

    public ClosedProducer(AbstractBenchmarkStressor stressor, long tt, int id) {
        super(id, stressor);
        this.thinkTime = tt;
    }

    @Override
    protected double getSleepTime() {
        return (double) thinkTime;
    }

    @Override
    protected void sleep() {
        synchronized(this){
            while( ! notified.compareAndSet(true,false) ){ // finchè non resetto (avoid spurious wakeups)
                try{
                    wait();
                } catch(InterruptedException e){
                    log.debug("Awakened");
                }
            }
        }
        // sleep think time
        try {
            Thread.sleep(thinkTime);
        } catch (InterruptedException e) {
            log.warn("Interrupt!");
        }
    }

    public void doNotify(){
        synchronized(this){
            notified.compareAndSet(false,true);
            notify();
        }
    }

    protected RequestType createRequestType(int reqType){
        return new RequestType(System.nanoTime(), reqType, this);
    }
}
