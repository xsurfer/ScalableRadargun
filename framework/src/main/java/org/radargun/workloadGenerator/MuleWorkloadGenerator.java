package org.radargun.workloadGenerator;

import org.radargun.CacheWrapper;
import org.radargun.stages.AbstractBenchmarkStage;
import org.radargun.stressors.producer.Producer;

import java.util.List;

/**
 * Created by: Fabio Perfetti
 * E-mail: perfabio87@gmail.com
 * Date: 3/18/13
 */
public class MuleWorkloadGenerator extends AbstractWorkloadGenerator {

    /* ****************** */
    /* *** ATTRIBUTES *** */
    /* ****************** */

    private long thinktime = 0;

    public MuleWorkloadGenerator(AbstractBenchmarkStage stage) {
        super(stage);
    }

    @Override
    public int getCurrentArrivalRate() {
        return 0;
    }

    @Override
    public List<Producer> createProducers(CacheWrapper cacheWrapper, int nodeIndex, double writeWeight, double readWeight) {
        throw new IllegalStateException("Mule system doesn't allow to create producers");
    }

    @Override
    public SystemType getSystemType(){ return SystemType.MULE; }

    @Override
    public MuleWorkloadGenerator clone(){
        return (MuleWorkloadGenerator) super.clone();
    }
}
