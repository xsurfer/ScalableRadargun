package org.radargun.stressors.producer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.stressors.AbstractBenchmarkStressor;

/**
 * Created by: Fabio Perfetti
 * E-mail: perfabio87@gmail.com
 * Date: 4/18/13
 */

public abstract class Producer extends Thread {

    protected static Log log = LogFactory.getLog(Producer.class);
    private boolean running = false;
    AbstractBenchmarkStressor stressor;

    public Producer(int id, AbstractBenchmarkStressor stressor) {
        super("Producer-" + id);
        this.stressor = stressor;
        setDaemon(true);
    }

    public void run() {
        if (log.isDebugEnabled()) {
            log.debug("Starting " + getName() + " with rate of " + getSleepTime());
        }
        while (assertRunning()) {

            stressor.addToQueue(stressor.nextTransaction());
            stressor.countJobs.incrementAndGet();
            sleep();
        }
    }

    protected abstract double getSleepTime();

    protected abstract void sleep();

    protected synchronized boolean assertRunning() {
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
