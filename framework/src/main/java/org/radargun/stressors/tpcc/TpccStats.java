package org.radargun.stressors.tpcc;

import org.radargun.stressors.commons.StressorStats;

/**
 * Created by: Fabio Perfetti
 * E-mail: perfabio87@gmail.com
 * Date: 4/2/13
 */
public class TpccStats extends StressorStats {

    public long writeInQueueTime = 0L;
    public long readInQueueTime = 0L;
    public long newOrderInQueueTime = 0L;
    public long paymentInQueueTime = 0L;

    public long numWriteDequeued = 0L;
    public long numReadDequeued = 0L;
    public long numNewOrderDequeued = 0L;
    public long numPaymentDequeued = 0L;

    private long localTimeout = 0L;
    private long remoteTimeout = 0L;
    private long numBackOffs = 0L;
    private long backedOffTime = 0L;

    private long reads = 0L;
    private long writes = 0L;
    private long payment = 0L;
    private long newOrder = 0L;

    private int nrFailures = 0;
    private int nrWrFailures = 0;
    private int nrWrFailuresOnCommit = 0;
    private int nrRdFailures = 0;
    private int nrNewOrderFailures = 0;
    private int nrPaymentFailures = 0;
    private int appFailures = 0;

    private long readDuration = 0L;
    private long writeDuration = 0L;
    private long newOrderDuration = 0L;
    private long paymentDuration = 0L;
    private long successful_commitWriteDuration = 0L;
    private long aborted_commitWriteDuration = 0L;
    private long commitWriteDuration = 0L;

    private long writeServiceTime = 0L;
    private long newOrderServiceTime = 0L;
    private long paymentServiceTime = 0L;
    private long readServiceTime = 0L;

    private long successful_writeDuration = 0L;
    private long successful_readDuration = 0L;

}
