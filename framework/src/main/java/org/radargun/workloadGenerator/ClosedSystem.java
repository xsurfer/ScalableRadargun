package org.radargun.workloadGenerator;

import org.radargun.stressors.BenchmarkStressor;
import org.radargun.stressors.consumer.Consumer;
import org.radargun.stressors.producer.Producer;

import java.util.List;
import java.util.Map;

/**
 * Created by: Fabio Perfetti
 * E-mail: perfabio87@gmail.com
 * Date: 4/19/13
 */
public class ClosedSystem implements IProducerSystem {

    private int population = 0;

    private long thinkTime = 0;

    @Override
    public String getType() {
        return SystemType.CLOSED;
    }

    @Override
    public Map<String, String> stress(BenchmarkStressor stressor) {
        return stressor.stress(this);
    }

    @Override
    public void consume(Consumer consumer) {
        consumer.consume(this);
    }

    @Override
    public void finishBenchmark(BenchmarkStressor stressor) {
        stressor.finishBenchmark(this);
    }

    @Override
    public List<Producer> createProducers(BenchmarkStressor stressor) {
        return stressor.createProducers(this);
    }


    /* *********************** */
    /* *** GETTERS/SETTERS *** */
    /* *********************** */

    public long getThinkTime(){ return thinkTime; }
    public void setThinkTime(long val){ thinkTime=val; }

    public long getPopulation(){ return population; }
    public void setPopulation(int val){ population = val; }



}
