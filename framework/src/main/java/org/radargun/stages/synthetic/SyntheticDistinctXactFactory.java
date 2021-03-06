package org.radargun.stages.synthetic;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.stages.stressors.syntethic.SyntheticParameters;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * // TODO: Document this
 *
 * @author diego
 * @since 4.0
 */
public class SyntheticDistinctXactFactory extends SyntheticXactFactory {

   private static Log log = LogFactory.getLog(SyntheticDistinctXactFactory.class);

   private boolean[] rwB;

   public SyntheticDistinctXactFactory(SyntheticParameters params, int threadIndex) {
      super(params, threadIndex);
      rwB = this.rwB();

      log.trace(Arrays.toString(rwB));
   }

   @Override
   protected XactOp[] buildReadWriteSet() {
      XactOp[] ops = new XactOp[rwB.length];
      List<Integer> readSet = new ArrayList<Integer>(), writeSet = new ArrayList<Integer>();
      int numReads = parameters.getUpdateXactReads();
      int numWrites = parameters.getUpdateXactWrites();
      int total = numReads + numWrites;
      //KeyGenerator kg = (KeyGenerator) Utils.instantiate(parameters.getKeyGeneratorClass());
      //Random r = parameters.getRna();
      int nodeIndex = parameters.getNodeIndex();
      //int threadIndex = parameters.getThreadIndex();
      int sizeS = parameters.getSizeOfAnAttribute();
      boolean bW = parameters.isAllowBlindWrites();
      int numK = parameters.getNumberOfAttributes();
      Integer key;
      int nextWrite = 0; //without blind writes, this points to the next read item to write
      //Generate rwSet
      try {
         for (int i = 0; i < total; i++) {
            if (!rwB[i]) {  //Read
               do {
                  key = rnd.nextInt(numK);
               }
               while (readSet.contains(key));  //avoid repetitions
               readSet.add(0, key);
               ops[i] = new XactOp(keyGenerator.generateKey(nodeIndex, threadIndex, key),
                                   null, false);    //add a read op and increment
            } else {    //Put
               if (bW) {        //You can have (distinct) blind writes
                  do {
                     key = rnd.nextInt(numK);
                  }
                  while (writeSet.contains(key));  //avoid repetitions among writes
                  writeSet.add(0, key);
                  ops[i] = new XactOp(keyGenerator.generateKey(nodeIndex, threadIndex, key),
                                      generateRandomString(sizeS), true);    //add a write op
               } else { //No blind writes: Take a value already read and increment         To have distinct writes, remember numWrites<=numReads in this case
                  ops[i] = new XactOp(ops[nextWrite++].getKey(),
                                      generateRandomString(sizeS), true);

                  while (nextWrite < total && rwB[nextWrite]) {       //while it is a put op, go on
                     nextWrite++;
                  }
               }
            }
         }
      } catch (Exception e) {
         e.printStackTrace();
      }
      log.trace(ops);
      return ops;
   }


   /**
    * @return A boolean array. True means put, false means get
    */
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
      try {
         //Set reads before first write
         for (; readI < fW; readI++) {
            rwB[readI] = false;
         }
         rwB[fW] = true;
         if (total == fW + 1)
            return rwB;
         double remainingReads = numReads - fW;
         double remainingWrites = numWrites - 1;
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
         int index = fW + 1;
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


}
