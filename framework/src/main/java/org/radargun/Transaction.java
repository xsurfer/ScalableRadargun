package org.radargun;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.stressors.AbstractBenchmarkStressor;
import org.radargun.stressors.producer.ClosedProducer;
import org.radargun.stressors.producer.RequestType;

import java.net.Authenticator;

/**
 * @author perfabio87@gmail.com
 */
public abstract class Transaction {

    private static Log log = LogFactory.getLog(Transaction.class);

    private RequestType requestType;

    private long enqueueTimestamp = -1;
    private long dequeueTimestamp = -1;

    private long startTimestamp = -1;
    private long endTimestamp = -1;

    private ClosedProducer producer;

    /* ******************* */
    /* *** CONSTRUCTOR *** */
    /* ******************* */

    public Transaction(){

    }


    /* ******************* */
    /* *** TO OVERRIDE *** */
    /* ******************* */

    public abstract void executeTransaction(CacheWrapper cacheWrapper) throws Throwable;

    public abstract boolean isReadOnly();

    public abstract int getType();


    /* *************** */
    /* *** METHODS *** */
    /* *************** */

    public void notifyProducer(){
        if( producer == null ) { log.warn("No producer to notify (open system?)"); return; }
        producer.doNotify();
    }

    /* *********************** */
    /* *** GETTERS/SETTERS *** */
    /* *********************** */

    public long getEnqueueTimestamp(){ if(enqueueTimestamp == -1) throw new IllegalArgumentException("enqueueTimestamp not set"); return enqueueTimestamp; }
    public void setEnqueueTimestamp(long val) { this.enqueueTimestamp = val; }

    public long getDequeueTimestamp(){ if(dequeueTimestamp == -1) throw new IllegalArgumentException("dequeueTimestamp not set"); return dequeueTimestamp; }
    public void setDequeueTimestamp(long val) { this.dequeueTimestamp = val; }

    public long getStartTimestamp(){ if(startTimestamp == -1) throw new IllegalArgumentException("startTimestamp not set"); return startTimestamp; }
    public void setStartTimestamp(long val) { this.startTimestamp = val; }

    public long getEndTimestamp(){ if(endTimestamp == -1) throw new IllegalArgumentException("endTimestamp not set"); return endTimestamp; }
    public void setEndTimestamp(long val) { this.endTimestamp = val; }

    public RequestType getRequestType(){ if(requestType == null) throw new IllegalArgumentException("requestType not set, it is mandatory!! Please add it using set method"); return requestType; }
    public void setRequestType(RequestType val) { this.requestType = val; }


}
