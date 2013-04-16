package org.radargun.stressors.tpcc;

import org.radargun.stressors.commons.StressorStats;

/**
 * Created by: Fabio Perfetti
 * E-mail: perfabio87@gmail.com
 * Date: 4/10/13
 */
public class TpccStats extends StressorStats {

    public static final String NR_NEW_ORDER_FAILURES = "nrNewOrderFailures";
    public static final String NR_PAYMENT_FAILURES = "nrPaymentFailures";
    public static final String PAYMENT = "payment";
    public static final String NEW_ORDER = "newOrder";
    public static final String NUM_NEW_ORDER_DEQUEUED = "numNewOrderDequeued";
    public static final String NUM_PAYMENT_DEQUEUED = "numPaymentDequeued";
    public static final String NEW_ORDER_IN_QUEUE_TIME = "newOrderInQueueTime";
    public static final String PAYMENT_IN_QUEUE_TIME = "paymentInQueueTime";
    public static final String NEW_ORDER_SERVICE_TIME = "newOrderServiceTime";
    public static final String PAYMENT_SERVICE_TIME = "paymentServiceTime";
    public static final String NEW_ORDER_DURATION = "newOrderDuration";
    public static final String PAYMENT_DURATION = "paymentDuration";

    public double evalNewOrderPerSec(){
        long denom = get(DURATION);
        if (denom == 0)
            return 0;
        return (double) ( get(NEW_ORDER) ) / (denom / 1000.0);
    }

    public double evalPaymentPerSec(){
        long denom = get(DURATION);
        if (denom == 0)
            return 0;
        return (double) ( get(PAYMENT) ) / (denom / 1000.0);
    }

    public double evalAvgNewOrderServiceTime(){
        long denom = get(NEW_ORDER) + get(NR_NEW_ORDER_FAILURES);
        if (denom == 0)
            return 0;
        return (double) ( get(NEW_ORDER_SERVICE_TIME) ) / (denom);
    }

    public double evalAvgPaymentServiceTime(){
        long denom = get(PAYMENT) + get(NR_PAYMENT_FAILURES);
        if (denom == 0)
            return 0;
        return (double) ( get(PAYMENT_SERVICE_TIME) ) / (denom);
    }

    public double evalAvgNewOrderInQueueTime(){
        long denom = get(NUM_NEW_ORDER_DEQUEUED);
        if (denom == 0)
            return 0;
        return (double) ( get(NEW_ORDER_IN_QUEUE_TIME) ) / (denom);
    }

    public double evalAvgPaymentInQueueTime(){
        long denom = get(NUM_PAYMENT_DEQUEUED);
        if (denom == 0)
            return 0;
        return (double) ( get(PAYMENT_IN_QUEUE_TIME) ) / (denom);
    }







}