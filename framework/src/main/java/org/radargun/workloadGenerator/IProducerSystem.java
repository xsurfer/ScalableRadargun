package org.radargun.workloadGenerator;

import org.radargun.CacheWrapper;
import org.radargun.stages.AbstractBenchmarkStage;
import org.radargun.stressors.BenchmarkStressor;
import org.radargun.stressors.StressorParameter;
import org.radargun.stressors.producer.Producer;

import java.util.List;

/**
 * Created by: Fabio Perfetti
 * E-mail: perfabio87@gmail.com
 * Date: 5/6/13
 */
public abstract interface IProducerSystem extends SystemType {

    public List<Producer> createProducers(CacheWrapper cacheWrapper, AbstractBenchmarkStage benchmarkStage, BenchmarkStressor stressor, StressorParameter parameters);

}
