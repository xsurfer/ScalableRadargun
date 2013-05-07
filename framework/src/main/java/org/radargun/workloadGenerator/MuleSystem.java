package org.radargun.workloadGenerator;

import org.radargun.stressors.BenchmarkStressor;
import org.radargun.stressors.consumer.Consumer;
import java.util.Map;

/**
 * Created by: Fabio Perfetti
 * E-mail: perfabio87@gmail.com
 * Date: 4/19/13
 */
public class MuleSystem implements SystemType {

    private long thinktime = 0;

    @Override
    public String getType() {
        return SystemType.MULE;
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

    public void setThinktime(long val){ thinktime = val; }
    public long getThinkTime(){ return thinktime; }

}
