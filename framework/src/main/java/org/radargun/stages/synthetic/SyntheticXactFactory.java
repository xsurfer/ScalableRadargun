package org.radargun.stages.synthetic;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.ITransaction;
import org.radargun.TransactionFactory;
import org.radargun.stages.stressors.KeyGenerator;
import org.radargun.stages.stressors.producer.RequestType;
import org.radargun.utils.Utils;

import java.util.Arrays;
import java.util.Random;

/**
 * // TODO: Document this
 *
 * @author diego
 * @since 4.0
 */
public abstract class SyntheticXactFactory implements TransactionFactory {
   protected static Log log = LogFactory.getLog(SyntheticXactFactory.class);

   protected Random rnd = new Random();
   protected KeyGenerator keyGenerator;
   protected int threadIndex;

   protected final SyntheticParameters parameters;

   protected boolean[] rwB;

   public SyntheticXactFactory(SyntheticParameters params, int threadIndex) {
      this.parameters = params;
      this.keyGenerator = (KeyGenerator) Utils.instantiate(params.getKeyGeneratorClass());
      this.threadIndex = threadIndex;
      if (parameters.getNumberOfAttributes() < parameters.getReadOnlyXactSize() ||
            parameters.getNumberOfAttributes() < parameters.getUpdateXactWrites()) {
         log.fatal("You cannot read distinct keys if the number of keys is less than the number of accesses");
         throw new IllegalArgumentException("You cannot read distinct keys if the number of keys is less than the number of accesses");
      }
      rwB = this.rwB();
      if (log.isTraceEnabled()) {
         log.trace(Arrays.toString(rwB));
      }
   }

   private boolean[] rwB() {
      int numReads = parameters.getUpdateXactReads();
      int numWrites = parameters.getUpdateXactWrites();
      int total = numReads + numWrites;
      boolean[] rwB = new boolean[total];
      String msgExc = null;
      int fW = parameters.getReadsBeforeFirstWrite();
      if (fW > numReads)
         msgExc = ("NumReadsBeforeFirstWrite > numReads!");
      if (numReads < numWrites && !parameters.isAllowBlindWrites())
         msgExc = "NumWrites has to be greater than numReads to avoid blindWrites and have no duplicates";
      if (fW == 0 && !parameters.isAllowBlindWrites())
         msgExc = ("Without blind writes you must at least read once before writing! NumReadsBeforeWrites at least 1!");
      if (msgExc != null) {
         log.fatal(msgExc);
         throw new RuntimeException(msgExc);
      }
      int readI = 0;
      final boolean readBeforeFirstWrite = fW != -1;
      try {
         double remainingReads = numReads;
         double remainingWrites = numWrites;
         if (readBeforeFirstWrite) {
            //If I have to read before doing the first write...
            for (; readI < fW; readI++) {
               rwB[readI] = false;
            }
            rwB[fW] = true;
            if (total == fW + 1)
               return rwB;
            remainingReads = numReads - fW;
            remainingWrites = numWrites - 1;
         }

         boolean moreReads = false;
         //If you have more remaining reads than writes, then each X reads you'll do ONE write; otherwise it's the opposite. If you have no more of one kind, you'll only have of the other one
         int groupRead, groupWrite, numGroups;
         if (remainingReads >= remainingWrites) {
            moreReads = true;
            groupRead = remainingWrites > 0 ? (int) Math.floor(remainingReads / remainingWrites) : (int) remainingReads;
            groupWrite = remainingWrites > 0 ? 1 : 0;
            numGroups = remainingWrites > 0 ? (int) remainingWrites : 1;
            log.trace("More remaining reads than write: " + remainingReads + " vs " + remainingWrites);
            log.trace("I will have " + numGroups + " groups of " + groupRead + " reads and " + groupWrite + " writes");
         } else {
            moreReads = false;
            groupRead = remainingReads > 0 ? 1 : 0;
            groupWrite = remainingReads > 0 ? (int) Math.floor(remainingWrites / remainingReads) : (int) remainingWrites;
            numGroups = remainingReads > 0 ? (int) remainingReads : 1;
            log.trace("More remaining writes than reads: " + remainingWrites + " vs " + remainingReads);
            log.trace("I will have " + numGroups + " groups of " + groupRead + " reads and " + groupWrite + " writes");
         }
         int index = readBeforeFirstWrite ? fW + 1 : 0;
         while (numGroups-- > 0) {
            log.trace(numGroups + " groups to go");
            int r = groupRead;
            int w = groupWrite;
            while (r-- > 0) {
               rwB[index++] = false;
            }
            while (w-- > 0) {
               rwB[index++] = true;
            }
         }
         while (index < total) {
            rwB[index++] = !moreReads;  //If you had more reads you have to top-up with writes(true) and vice versa
         }
      } catch (Exception e) {
         e.printStackTrace();
      }

      return rwB;

   }

   @Override
   public ITransaction generateTransaction(RequestType type) {

      if (type.getTransactionType() == xactClass.RO.getId()) {
         return generateROXact();
      } else {
         return generateUPXact();
      }
   }

   @Override
   public int nextTransaction() {
      if (!parameters.getCacheWrapper().isTheMaster() || (1 + rnd.nextInt(100)) > parameters.getWritePercentage())
         return xactClass.RO.getId();
      return xactClass.WR.getId();
   }

   @Override
   public ITransaction choiceTransaction() {
      int txType = nextTransaction();
      RequestType requestType = new RequestType(System.nanoTime(), txType);

      return generateTransaction(requestType);
   }

   protected final String generateRandomString(int size) {
      // each char is 2 bytes
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < size / 2; i++) sb.append((char) (64 + rnd.nextInt(26)));
      return sb.toString();
   }

   protected abstract ITransaction generateROXact();

   protected abstract ITransaction generateUPXact();

}
