package org.radargun.tpcc.domain;

import org.radargun.CacheWrapper;
import org.radargun.tpcc.DomainObject;

/**
 * @author Diego Didona, didona@gsd.inesc-id.pt
 *         Date: 20/12/12
 */
public abstract class AbstractDomainObject implements DomainObject {


   protected abstract Object getKey();

   protected abstract Object generateId(int slaveIndex);

   public final void threadAwareStore(CacheWrapper wrapper, int threadId) throws Throwable {
      wrapper.put(null, this.getKey(), this, threadId);
   }

   //This will be called only by History
   public final void threadAwareStore(CacheWrapper wrapper, int slaveId, int threadId) throws Throwable {
      String id = (String) generateId(slaveId);
      wrapper.put(null, id, this, threadId);
   }


}
