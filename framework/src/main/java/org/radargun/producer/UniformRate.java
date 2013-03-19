package org.radargun.producer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Created by: Fabio Perfetti
 * E-mail: perfabio87@gmail.com
 * Date: 3/19/13
 */
public class UniformRate extends ProducerRate {

    private static Log log = LogFactory.getLog(UniformRate.class);

    /**
     * @param producerLambda the lambda (arrival rate) in transaction per millisecond
     */
    public UniformRate(double producerLambda) {
        super(producerLambda);
    }

    @Override
    public double timeToSleep(double lambda) {
        double ret = 1 / lambda;
        return ret;
    }
}
