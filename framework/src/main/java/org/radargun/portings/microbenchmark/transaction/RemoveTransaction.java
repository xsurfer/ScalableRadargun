package org.radargun.portings.microbenchmark.transaction;

import org.radargun.CacheWrapper;
import org.radargun.portings.microbenchmark.domain.IntSet;

public class RemoveTransaction implements MicrobenchmarkTransaction {

   public final int value;

   public RemoveTransaction(int value) {
      this.value = value;
   }

   @Override
   public boolean executeTransaction(CacheWrapper cacheWrapper) throws Throwable {
      IntSet intset = ((IntSet) cacheWrapper.get(null, "SET"));
      boolean res = intset.remove(cacheWrapper, this.value);
      return res;
   }

   @Override
   public boolean isReadOnly() {
      return false;
   }

}
