package org.radargun.stages.stressors.systems;

/**
 * Created by: Fabio Perfetti
 * E-mail: perfabio87@gmail.com
 * Date: 5/9/13
 */

public enum RateDistribution {
    UNIFORM("UniformRate"), EXPONENTIAL("ExponentialRate");
    private String value;

    RateDistribution(String value) {
        this.value = value;
    }

    public String getDistributionRateName(){ return this.value; }

}
