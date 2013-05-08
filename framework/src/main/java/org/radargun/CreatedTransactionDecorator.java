package org.radargun;

/**
 * Created by: Fabio Perfetti
 * E-mail: perfabio87@gmail.com
 * Date: 5/8/13
 */
public class CreatedTransactionDecorator extends TransactionDecorator {

    // emulating no queue system

    public CreatedTransactionDecorator(ITransaction transaction) {
        super(transaction);
    }

    @Override
    public long getEnqueueTimestamp() {
        return getStartTimestamp();
    }

    @Override
    public long getDequeueTimestamp() {
        return getStartTimestamp();
    }


}
