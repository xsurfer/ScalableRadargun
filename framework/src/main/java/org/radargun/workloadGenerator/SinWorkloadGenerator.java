package org.radargun.workloadGenerator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.stages.AbstractBenchmarkStage;

/**
 * Generate a workload based on function f(t)=amplitude*sin(t)
 * User: Fabio Perfetti
 * Date: 3/18/13
 */
public class SinWorkloadGenerator extends AbstractWorkloadGenerator {

    private static Log log = LogFactory.getLog(SinWorkloadGenerator.class);

    private double amplitude = 1.0;

    public SinWorkloadGenerator(AbstractBenchmarkStage stage) {
        super(stage);
    }

    @Override
    public int getCurrentArrivalRate() {
        int eval = (int) Math.abs(Math.ceil(amplitude * Math.sin(getTime())));
        return eval;
    }

    public double getAmplitude(){ return this.amplitude; }
    public void setAmplitude(double amplitude){ this.amplitude = amplitude; }

    @Override
    public SinWorkloadGenerator clone(){
        return (SinWorkloadGenerator) super.clone();
    }

}