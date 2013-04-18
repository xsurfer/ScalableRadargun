package org.radargun.workloadGenerator;

import org.radargun.CacheWrapper;
import org.radargun.stages.AbstractBenchmarkStage;
import org.radargun.stressors.producer.Producer;

import java.util.List;

/**
 * Created by: Fabio Perfetti
 * E-mail: perfabio87@gmail.com
 * Date: 4/18/13
 */
public abstract class AbstractOpenWorkloadGenerator extends AbstractWorkloadGenerator {

    public AbstractOpenWorkloadGenerator(AbstractBenchmarkStage stage) {
        super(stage);
    }

    @Override
    public List<Producer> createProducers(CacheWrapper cacheWrapper, int nodeIndex, double writeWeight, double readWeight) {
        return null;
    }

    @Override
    public SystemType getSystemType() {
        return SystemType.OPEN;
    }
}
