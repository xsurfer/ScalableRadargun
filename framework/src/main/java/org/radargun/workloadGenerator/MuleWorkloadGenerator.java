package org.radargun.workloadGenerator;

import org.radargun.stages.AbstractBenchmarkStage;

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
    public SystemType getSystemType(){ return SystemType.MULE; }

    @Override
    public MuleWorkloadGenerator clone(){
        return (MuleWorkloadGenerator) super.clone();
    }
}
