package org.radargun.stages;

import org.radargun.DistStage;
import org.radargun.workloadGenerator.AbstractWorkloadGenerator;

/**
 * Created by: Fabio Perfetti
 * E-mail: perfabio87@gmail.com
 * Date: 3/18/13
 */
public abstract class BenchmarkStage extends AbstractDistStage {

    /**
     * the workload generator
     */
    AbstractWorkloadGenerator workloadGenerator;

    public void setWorkloadGenerator(AbstractWorkloadGenerator wg){ this.workloadGenerator = wg; }
    public AbstractWorkloadGenerator getWorkloadGenerator(){ return this.workloadGenerator; }

    public BenchmarkStage clone() {

        BenchmarkStage clone;
        clone = (BenchmarkStage) super.clone();
        clone.workloadGenerator = workloadGenerator.clone();
        return clone;

    }



}
