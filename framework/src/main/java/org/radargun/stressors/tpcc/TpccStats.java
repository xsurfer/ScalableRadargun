package org.radargun.stressors.tpcc;

import org.radargun.stressors.commons.StressorStats;

/**
 * Created by: Fabio Perfetti
 * E-mail: perfabio87@gmail.com
 * Date: 4/10/13
 */
public class TpccStats extends StressorStats {

    private int nrNewOrderFailures = 0;
    public int getNrNewOrderFailures() { return nrNewOrderFailures; }
    public void incNrNewOrderFailures(){ nrNewOrderFailures++; }

    private int nrPaymentFailures = 0;
    public int getNrPaymentFailures() { return nrPaymentFailures; }
    public void incNrPaymentFailures(){ nrPaymentFailures++; }

    private long payment = 0L;
    public long getPayment() { return payment; }
    public void incPayment(){ payment++; }

    private long newOrder = 0L;
    public long getNewOrder() { return newOrder; }
    public void incNewOrder(){ newOrder++; }

    private long numNewOrderDequeued = 0L;
    public long getNumNewOrderDequeued() { return numNewOrderDequeued; }
    public void incNumNewOrderDequeued(){ numNewOrderDequeued++; }

    private long numPaymentDequeued = 0L;
    public long getNumPaymentDequeued() { return numPaymentDequeued; }
    public void incNumPaymentDequeued(){ numPaymentDequeued++; }

    private long newOrderInQueueTime = 0L;
    public long getNewOrderInQueueTime() { return newOrderInQueueTime; }
    public void incNewOrderInQueueTime(){ newOrderInQueueTime++; }

    private long paymentInQueueTime = 0L;
    public long getPaymentInQueueTime() { return paymentInQueueTime; }
    public void incPaymentInQueueTime(){ paymentInQueueTime++; }

    private long newOrderServiceTime = 0L;
    public long getNewOrderServiceTime() { return newOrderServiceTime; }
    public void incNewOrderServiceTime(){ newOrderServiceTime++; }

    private long paymentServiceTime = 0L;
    public long getPaymentServiceTime() { return paymentServiceTime; }
    public void incPaymentServiceTime(){ paymentServiceTime++; }

    private long newOrderDuration = 0L;
    public long getNewOrderDuration() { return newOrderDuration; }
    public void incNewOrderDuration(){ newOrderDuration++; }

    private long paymentDuration = 0L;
    public long getPaymentDuration() { return paymentDuration; }
    public void incPaymentDuration(){ paymentDuration++; }

}