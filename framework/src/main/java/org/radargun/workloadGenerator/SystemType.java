package org.radargun.workloadGenerator;

import org.radargun.CacheWrapper;
import org.radargun.stages.AbstractBenchmarkStage;
import org.radargun.stressors.BenchmarkStressor;
import org.radargun.stressors.StressorParameter;
import org.radargun.stressors.consumer.Consumer;
import java.util.Map;

/**
 * Created by: Fabio Perfetti
 * E-mail: perfabio87@gmail.com
 * Date: 4/19/13
 */
public interface SystemType {

    public final static String OPEN = "open";
    public final static String CLOSED = "closed";
    public final static String MULE = "mule";

    public String getType();

    //public Consumer createConsumer(CacheWrapper cacheWrapper, int threadIndex, AbstractBenchmarkStage benchmarkStage, BenchmarkStressor stressor, StressorParameter parameters);

    public Map<String, String> stress(BenchmarkStressor stressor);

    public void consume(Consumer consumer);

    public void finishBenchmark(BenchmarkStressor stressor);
}
