package org.radargun.workloadGenerator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.CacheWrapper;
import org.radargun.stages.AbstractBenchmarkStage;
import org.radargun.stressors.BenchmarkStressor;
import org.radargun.stressors.StressorParameter;
import org.radargun.stressors.consumer.Consumer;
import org.radargun.stressors.producer.GroupProducerRateFactory;
import org.radargun.stressors.producer.OpenProducer;
import org.radargun.stressors.producer.Producer;
import org.radargun.stressors.producer.ProducerRate;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by: Fabio Perfetti
 * E-mail: perfabio87@gmail.com
 * Date: 4/19/13
 */
public class OpenSystem implements IProducerSystem {

    private static Log log = LogFactory.getLog(OpenSystem.class);

    private AbstractWorkloadGenerator workloadGenerator;

    public AbstractWorkloadGenerator getWorkloadGenerator(){ return workloadGenerator; }

    @Override
    public String getType() {
        return SystemType.OPEN;
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
}
