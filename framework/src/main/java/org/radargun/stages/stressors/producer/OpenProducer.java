package org.radargun.stages.stressors.producer;

import org.radargun.stages.stressors.AbstractBenchmarkStressor;

/**
 * Created by: Fabio Perfetti
 * E-mail: perfabio87@gmail.com
 * Date: 4/18/13
 */

public class OpenProducer extends Producer {
    private final ProducerRate rate;

    public OpenProducer(AbstractBenchmarkStressor stressor, ProducerRate rate, int id) {
        super(id, stressor);
        this.rate = rate;
    }

    @Override
    protected double getSleepTime() {
        return rate.getLambda();
    }

    @Override
    protected void sleep() {
        rate.sleep();
    }
}