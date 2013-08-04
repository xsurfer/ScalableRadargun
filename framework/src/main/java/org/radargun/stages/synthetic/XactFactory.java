package org.radargun.stages.synthetic;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.TransactionFactory;
import org.radargun.stages.stressors.Parameter;

/**
 * // TODO: Document this
 *
 * @author diego
 * @since 4.0
 */
public abstract class XactFactory <T extends Parameter, R>  {


   protected final T params;
   protected final static Log log = LogFactory.getLog(XactFactory.class);

   protected XactFactory(T params) {
      this.params = params;
      log.trace("Factory built "+params);
   }

   public abstract R createTransaction(int txId);
}
