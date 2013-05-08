package org.radargun;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.stages.stressors.producer.ClosedProducer;
import org.radargun.stages.stressors.producer.RequestType;

/**
 * @author perfabio87@gmail.com
 */
public abstract class TransactionDecorator implements ITransaction {

    private static Log log = LogFactory.getLog(TransactionDecorator.class);

    protected ITransaction tx;

    protected long dequeueTimestamp = -1;

    private long startTimestamp = -1;

    private long endTimestamp = -1;

    /* ******************* */
    /* *** CONSTRUCTOR *** */
    /* ******************* */

    public TransactionDecorator(ITransaction transaction){
        tx = transaction;
    }

    /* *************** */
    /* *** METHODS *** */
    /* *************** */

    public void regenerate(ITransaction newTx){
        this.tx = newTx;
    }

    /* ****************** */
    /* *** OVERRIDING *** */
    /* ****************** */

    public void executeTransaction(CacheWrapper cacheWrapper) throws Throwable{
        tx.executeTransaction(cacheWrapper);
    }

    public boolean isReadOnly(){
        return tx.isReadOnly();
    }

    public int getType(){
        return tx.getType();
    }


    /* ******************* */
    /* *** TO OVERRIDE *** */
    /* ******************* */

    public abstract long getEnqueueTimestamp();

    public abstract long getDequeueTimestamp();


    /* *********************** */
    /* *** GETTERS/SETTERS *** */
    /* *********************** */

    public ITransaction getInnerTransaction(){ return tx; }

    public long getStartTimestamp(){ if(startTimestamp == -1) throw new IllegalArgumentException("startTimestamp not set"); return startTimestamp; }
    public void setStartTimestamp(long val) { this.startTimestamp = val; }

    public long getEndTimestamp(){ if(endTimestamp == -1) throw new IllegalArgumentException("endTimestamp not set"); return endTimestamp; }
    public void setEndTimestamp(long val) { this.endTimestamp = val; }

}
