package org.radargun.stressors.producer;

import org.radargun.stressors.BenchmarkStressor;

/**
 * Created by: Fabio Perfetti
 * E-mail: perfabio87@gmail.com
 * Date: 4/18/13
 */

public class OpenProducer extends Producer {
    private final ProducerRate rate;

    public OpenProducer(BenchmarkStressor stressor, ProducerRate rate, int id) {
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