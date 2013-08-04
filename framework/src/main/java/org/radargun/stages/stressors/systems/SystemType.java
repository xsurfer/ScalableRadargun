package org.radargun.stages.stressors.systems;

import org.radargun.stages.stressors.AbstractBenchmarkStressor;
import org.radargun.stages.stressors.consumer.Consumer;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by: Fabio Perfetti
 * E-mail: perfabio87@gmail.com
 * Date: 4/19/13
 */
public interface SystemType extends Serializable {

    public final static String OPEN = "open";
    public final static String CLOSED = "closed";
    public final static String MULE = "mule";

    public String getType();

    //public Consumer createConsumer(CacheWrapper cacheWrapper, int threadIndex, AbstractBenchmarkStage benchmarkStage, AbstractBenchmarkStressor stressor, Parameter parameters);

    public Map<String, String> stress(AbstractBenchmarkStressor stressor);

    public void consume(Consumer consumer);

    public void finishBenchmark(AbstractBenchmarkStressor stressor);
}
