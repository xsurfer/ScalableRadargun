package org.radargun.stages.stressors.producer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.stages.stressors.AbstractBenchmarkStressor;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by: Fabio Perfetti
 * E-mail: perfabio87@gmail.com
 * Date: 4/18/13
 */

public abstract class Producer implements Runnable {

    protected static Log log = LogFactory.getLog(Producer.class);
    private AtomicBoolean running = new AtomicBoolean(false);
    AbstractBenchmarkStressor stressor;

    public Producer(int id, AbstractBenchmarkStressor stressor) {
        this.stressor = stressor;

    }

    public void run() {
        if( !running.compareAndSet(false,true) ){
            log.warn("Someone is trying to execute a producer already started!! " +
                    "Thread name:" + Thread.currentThread().getName() + ". Skipping");
            return;
        } else {
            if (log.isDebugEnabled()) {
                log.debug("Starting " + Thread.currentThread().getName() + " with rate of " + getSleepTime());
            }
        }

        while ( running.get() ) {

            int reqType = stressor.nextTransaction();
            RequestType request = createRequestType(reqType);

            stressor.addToQueue(request);
            stressor.countJobs.incrementAndGet();
            sleep();
            // chiamando interrupt si imposta la variable running a false, facendo uscire il thread dal ciclo.
            // >>> PER EVITARE ULTERIORI GIRI DA PARTE DEL PRODUCER, TENERE LA CHIAMATA sleep() A FONDO CICLO <<<
        }
        log.info(Thread.currentThread().getName() + " ended");
    }

    protected abstract double getSleepTime();

    protected abstract void sleep();

    protected abstract RequestType createRequestType(int reqType);

    public abstract void doNotify();


    public void interrupt() {
        if( running.compareAndSet(true,false) ){
            RequestType request = createRequestType(9999);
            stressor.addToQueue(request);
            // ora il producer è pronto per morire
        }
    }
}
