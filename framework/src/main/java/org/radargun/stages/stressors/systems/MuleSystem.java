package org.radargun.stages.stressors.systems;

import org.radargun.stages.stressors.AbstractBenchmarkStressor;
import org.radargun.stages.stressors.consumer.Consumer;

import java.util.Map;

/**
 * Created by: Fabio Perfetti
 * E-mail: perfabio87@gmail.com
 * Date: 4/19/13
 */
public class MuleSystem implements System {

    private long thinkTime = 0;

    private RateDistribution rateDistribution = RateDistribution.EXPONENTIAL;

    @Override
    public SystemType getType() {
        return SystemType.MULE;
    }

    @Override
    public Map<String, String> stress(AbstractBenchmarkStressor stressor) {
        return stressor.stress(this);
    }

    @Override
    public void consume(Consumer consumer) {
        consumer.consume(this);
    }

    @Override
    public void finishBenchmark(AbstractBenchmarkStressor stressor) {
        stressor.finishBenchmark(this);
    }

    public void setThinkTime(long val){ thinkTime = val; }
    public long getThinkTime(){ return thinkTime; }

    public RateDistribution getRateDistribution(){ return this.rateDistribution; }
    public void setRateDistribution(String rate){
        rateDistribution = RateDistribution.valueOf(rate.toUpperCase());
    }

}
