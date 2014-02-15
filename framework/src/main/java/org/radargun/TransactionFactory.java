package org.radargun;

import org.radargun.stages.stressors.producer.RequestType;

/**
 * Author: Fabio Perfetti (perfabio87 [at] gmail.com) Date: 8/3/13 Time: 6:49 PM
 */
public interface TransactionFactory {

   public int nextTransaction();

   public abstract ITransaction generateTransaction(RequestType type);

   public abstract ITransaction choiceTransaction();

}
