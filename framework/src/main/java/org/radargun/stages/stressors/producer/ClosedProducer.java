package org.radargun.stages.stressors.producer;

import org.radargun.TransactionFactory;
import org.radargun.stages.stressors.AbstractBenchmarkStressor;
import org.radargun.stages.stressors.Parameters;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by: Fabio Perfetti
 * E-mail: perfabio87@gmail.com
 * Date: 4/18/13
 */
public class ClosedProducer extends OpenProducer {

    private long thinkTime;
    private AtomicBoolean notified = new AtomicBoolean(false);


    public ClosedProducer(AbstractBenchmarkStressor stressor, ProducerRate rate, int id, Parameters parameters, TransactionFactory factory) {
        super(stressor, rate, id, parameters, factory);
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
        rate.sleep();
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
