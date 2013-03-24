package org.radargun.producer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Random;

/**
 * Created by: Fabio Perfetti
 * E-mail: perfabio87@gmail.com
 * Date: 3/19/13
 */
public class ExponentialRate extends ProducerRate {

    private static Log log = LogFactory.getLog(ExponentialRate.class);
    private final Random random;

    /**
     * @param producerLambda the lambda (arrival rate) in transaction per millisecond
     */
    public ExponentialRate(double producerLambda) {
        super(producerLambda);
        this.random = new Random(System.currentTimeMillis());
    }

    /**
     * NOTE: the public visibility is only for testing purpose!
     *
     * @param lambda the lambda in milliseconds
     * @return the sleeping time in milliseconds
     */
    @Override
    public double timeToSleep(double lambda) {
        double ret = -Math.log(1.0D - random.nextDouble()) / lambda;
        //I bound the value in the interval 1msec-30sec--> at least 1msec!
        return ret < 1 ? 1 : (ret > 30000 ? 30000 : ret);
    }
}
