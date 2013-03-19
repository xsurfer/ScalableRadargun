package org.radargun.workloadGenerator;

/**
 * Created by: Fabio Perfetti
 * E-mail: perfabio87@gmail.com
 * Date: 3/18/13
 */
public class ClosedWorkloadGenerator extends AbstractWorkloadGenerator {

    @Override
    public int getCurrentArrivalRate() {
        return 0;
    }

    @Override
    public boolean isOpenSystem(){ return false; }

    @Override
    public ClosedWorkloadGenerator clone(){
        return (ClosedWorkloadGenerator) super.clone();
    }
}
