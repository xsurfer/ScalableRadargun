package org.radargun;

import org.radargun.stages.stressors.producer.RequestType;

/**
 * Created by: Fabio Perfetti
 * E-mail: perfabio87@gmail.com
 * Date: 5/8/13
 */
public class GeneratedTransactionDecorator extends TransactionDecorator {

    private final RequestType requestType;

    protected long dequeueTimestamp;

    public GeneratedTransactionDecorator(ITransaction tx, RequestType rq, long dequeueTimestamp){
        super(tx);
        requestType = rq;
        this.dequeueTimestamp = dequeueTimestamp;
    }

//    public void notifyProducer(){
//        requestType.getProducer().doNotify();
//    }

    public long getEnqueueTimestamp(){ return requestType.getEnqueueTimestamp(); }

    public long getDequeueTimestamp(){ return dequeueTimestamp; }

    public RequestType getRequestType(){ return requestType; }

}
