package org.radargun.stressors.commons;

/**
 * Created by: Fabio Perfetti
 * E-mail: perfabio87@gmail.com
 * Date: 4/2/13
 */
public class StressorStats {
    private int nrFailures = 0;
    public void incrementNrFailures(){ nrFailures++; }

    private int nrWrFailures = 0;
    public void incrementNrWrFailures(){ nrWrFailures++; }

    private int nrWrFailuresOnCommit = 0;
    public void incrementNrWrFailuresOnCommit(){ nrWrFailuresOnCommit++; }

    private int nrRdFailures = 0;
    public void incrementNrRdFailures(){ nrRdFailures++; }

    private int nrNewOrderFailures = 0;
    public void incrementNrNewOrderFailures(){ nrNewOrderFailures++; }

    private int nrPaymentFailures = 0;
    public void incrementNrPaymentFailures(){ nrPaymentFailures++; }

    private int appFailures = 0;
    public void incrementAppFailures(){ appFailures++; }

    private long readDuration = 0L;
    public void incrementReadDuration(){ readDuration++; }

    private long writeDuration = 0L;
    public void incrementWriteDuration(){ writeDuration++; }

    private long newOrderDuration = 0L;
    public void incrementNewOrderDuration(){ newOrderDuration++; }

    private long paymentDuration = 0L;
    public void incrementPaymentDuration(){ paymentDuration++; }

    private long successfulCommitWriteDuration = 0L;
    public void incrementSuccessfulCommitWriteDuration(){ successfulCommitWriteDuration++; }

    private long abortedCommitWriteDuration = 0L;
    public void incrementAbortedCommitWriteDuration(){ abortedCommitWriteDuration++; }

    private long commitWriteDuration = 0L;
    public void incrementCommitWriteDuration(){ commitWriteDuration++; }

    private long writeServiceTime = 0L;
    public void incrementWriteServiceTime(){ writeServiceTime++; }

    private long newOrderServiceTime = 0L;
    public void incrementNewOrderServiceTime(){ newOrderServiceTime++; }

    private long paymentServiceTime = 0L;
    public void incrementPaymentServiceTime(){ paymentServiceTime++; }

    private long readServiceTime = 0L;
    public void incrementReadServiceTime(){ readServiceTime++; }

    private long successfulWriteDuration = 0L;
    public void incrementSuccessfulWriteDuration(){ successfulWriteDuration++; }

    private long successfulReadDuration = 0L;
    public void incrementSuccessfulReadDuration(){ successfulReadDuration++; }

    private long reads = 0L;
    public void incrementReads(){ reads++; }

    private long writes = 0L;
    public void incrementWrites(){ writes++; }

    private long payment = 0L;
    public void incrementPayment(){ payment++; }

    private long newOrder = 0L;
    public void incrementNewOrder(){ newOrder++; }

    private long numWriteDequeued = 0L;
    public void incrementNumWriteDequeued(){ numWriteDequeued++; }

    private long numReadDequeued = 0L;
    public void incrementNumReadDequeued(){ numReadDequeued++; }

    private long numNewOrderDequeued = 0L;
    public void incrementNumNewOrderDequeued(){ numNewOrderDequeued++; }

    private long numPaymentDequeued = 0L;
    public void incrementNumPaymentDequeued(){ numPaymentDequeued++; }

    private long writeInQueueTime = 0L;
    public void incrementWriteInQueueTime(){ writeInQueueTime++; }

    private long readInQueueTime = 0L;
    public void incrementReadInQueueTime(){ readInQueueTime++; }

    private long newOrderInQueueTime = 0L;
    public void incrementNewOrderInQueueTime(){ newOrderInQueueTime++; }

    private long paymentInQueueTime = 0L;
    public void incrementPaymentInQueueTime(){ paymentInQueueTime++; }

    private long localTimeout = 0L;
    public void incrementLocalTimeout(){ localTimeout++; }

    private long remoteTimeout = 0L;
    public void incrementRemoteTimeout(){ remoteTimeout++; }

    private long numBackOffs = 0L;
    public void incrementNumBackOffs(){ numBackOffs++; }

    private long backedOffTime = 0L;
    public void incrementBackedOffTime(){ backedOffTime++; }

}
