package org.radargun.stages.synthetic.runTimeDap;

import org.radargun.ITransaction;
import org.radargun.stages.synthetic.SyntheticParameters;
import org.radargun.stages.synthetic.SyntheticXact;
import org.radargun.stages.synthetic.SyntheticXactFactory;

/**
 * // TODO: Document this
 *
 * @author diego
 * @since 4.0
 */
public class SyntheticXactFactory_RunTimeDaP extends SyntheticXactFactory {


   public SyntheticXactFactory_RunTimeDaP(SyntheticParameters params, int threadIndex) {
      super(params, threadIndex);
      sanityCheck();
   }


   protected void sanityCheck() {
      if (parameters.getUpdateXactReads() < parameters.getUpdateXactWrites())
         throw new IllegalArgumentException("For now, numWrites has to be <= numReads");
      //TODO: check in the factory this
      //if (parameters.getXact_retry().equals(XACT_RETRY.RETRY_SAME_XACT))
      // throw new IllegalArgumentException("For now, runTimeDap transactions cannot be exactly retried. Only no_retry or same_class is allowed");
      //TODO I just have to do the following: populate at runtime the XactOps, so that it can be injected. I just have to know if I am
      //TODO a retried transaction, so that I can use the XactOps instead of computing it at runtime

   }

   @Override
   protected ITransaction generateROXact() {
      SyntheticXact xact = new SyntheticXact(true);
      xact.setSpinBetweenOps(parameters.getSpinBetweenOps());
      xact.setIterator(new IteratorRuntimeDap_RO(parameters, keyGenerator, rnd, threadIndex));
      return xact;
   }

   @Override
   protected ITransaction generateUPXact() {
      SyntheticXact xact = new SyntheticXact(false);
      xact.setSpinBetweenOps(parameters.getSpinBetweenOps());
      xact.setIterator(new IteratorRuntimeDap_UP(parameters, rwB, keyGenerator, rnd, threadIndex));
      return xact;
   }


}
