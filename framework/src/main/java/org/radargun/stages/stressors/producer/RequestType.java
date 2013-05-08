package org.radargun.stages.stressors.producer;

/**
 * Created by: Fabio Perfetti
 * E-mail: perfabio87@gmail.com
 * Date: 4/18/13
 */
public class RequestType {

    private final long enqueueTimestamp;
    private final int transactionType;
    private Producer producer;

    public RequestType(long timestamp, int transactionType) {
        this.enqueueTimestamp = timestamp;
        this.transactionType = transactionType;
    }

    public RequestType(long timestamp, int transactionType, ClosedProducer producer) {
        this.enqueueTimestamp = timestamp;
        this.transactionType = transactionType;
        this.producer = producer;
    }

    public long getEnqueueTimestamp(){ return enqueueTimestamp; }

    public int getTransactionType(){ return transactionType; }

    public Producer getProducer(){ return producer; }

}