package org.radargun.stages.stressors.producer;

/**
 * Created by: Fabio Perfetti
 * E-mail: perfabio87@gmail.com
 * Date: 4/18/13
 */
public class RequestType {

    public long enqueueTimestamp;
    public int transactionType;

    public boolean notifiable = false;
    public ClosedProducer producer;

    public RequestType(long timestamp, int transactionType) {
        this.enqueueTimestamp = timestamp;
        this.transactionType = transactionType;
    }

    public RequestType(long timestamp, int transactionType, ClosedProducer producer) {
        this.enqueueTimestamp = timestamp;
        this.transactionType = transactionType;
        this.notifiable = true;
    }
}