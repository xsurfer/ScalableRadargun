package org.radargun.stages.synthetic;

import org.radargun.CacheWrapper;
import org.radargun.ITransaction;

import java.util.Iterator;

/**
 * // TODO: Document this
 *
 * @author diego
 * @since 4.0
 */
public class SyntheticXact implements ITransaction {

   private long initResponseTime;
   private long initServiceTime;
   public int clazz;
   private boolean isCommit;  //we track only commit-abort without considering also xact that can abort because of application logic (and might be not restarted, then)
   private long spinBetweenOps;

   private boolean isReadOnly;
   private Iterator<XactOp> iterator;


   public SyntheticXact(boolean isreadOnly) {
      initResponseTime = System.nanoTime();
      initServiceTime = initResponseTime;
      this.isReadOnly = isreadOnly;
   }


   public void setSpinBetweenOps(long spinBetweenOps) {
      this.spinBetweenOps = spinBetweenOps;
   }

   public long getInitResponseTime() {
      return initResponseTime;
   }

   public void setInitResponseTime(long initResponseTime) {
      this.initResponseTime = initResponseTime;
   }

   public long getInitServiceTime() {
      return initServiceTime;
   }

   public void setInitServiceTime(long initServiceTime) {
      this.initServiceTime = initServiceTime;
   }

   public int getClazz() {
      return clazz;
   }

   public void setClazz(int clazzId) {
      this.clazz = clazzId;
   }

   public boolean isCommit() {
      return isCommit;
   }

   public void setCommit(boolean commit) {
      isCommit = commit;
   }


   @Override
   public String toString() {
      return "SyntheticXact{" +
            "initResponseTime=" + initResponseTime +
            ", initServiceTime=" + initServiceTime +
            ", clazz=" + clazz +
            ", isCommit=" + isCommit +
            '}';
   }

   @Override
   public void executeTransaction(CacheWrapper cache) throws Throwable {
      XactOp op;
      final boolean isSpin = spinBetweenOps > 0;
      while (iterator.hasNext()) {
         op = iterator.next();
         if (op.isPut())
            cache.put(null, op.getKey(), op.getValue());
         else
            cache.get(null, op.getKey());
         if (isSpin)
            doSpin(spinBetweenOps);
      }
   }

   private void doSpin(long spin) {
      for (long l = 0; l < spin; l++) ;
   }

   @Override
   public boolean isReadOnly() {
      return isReadOnly;
   }

   @Override
   public int getType() {
      return 0;
   }

   public void setIterator(Iterator<XactOp> iterator) {
      this.iterator = iterator;
   }

}