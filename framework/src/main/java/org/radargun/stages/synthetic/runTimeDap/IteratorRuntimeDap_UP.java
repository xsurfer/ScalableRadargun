package org.radargun.stages.synthetic.runTimeDap;

import org.radargun.stages.stressors.KeyGenerator;
import org.radargun.stages.synthetic.SyntheticParameters;
import org.radargun.stages.synthetic.XactOp;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Random;
import java.util.Set;

/**
 * // TODO: Document this
 *
 * @author diego
 * @since 4.0
 */
public class IteratorRuntimeDap_UP implements Iterator<XactOp> {

   protected LinkedList<Integer> readSet = new LinkedList<Integer>();
   protected Set<Integer> writeSet = new HashSet<Integer>();
   protected SyntheticParameters params;
   protected boolean[] rwB;
   protected int currentOp = 0;
   protected KeyGenerator keyGen;
   protected Random r;
   protected int numKeys;
   protected int nodeIndex;
   protected int threadIndex;
   protected boolean blindWriteAllowed = false;
   protected int sizeOfAttribute;
   protected int indexNextWrite = 0; //This points to the (i-1)-st read performed, in order to always write on distinct read items
   private final int toDo;


   public IteratorRuntimeDap_UP(SyntheticParameters params, boolean[] b, KeyGenerator keyGenerator, Random rnd, int threadIndex) {
      this.params = params;
      r = rnd;
      numKeys = params.getNumberOfAttributes();
      nodeIndex = params.getNodeIndex();
      this.threadIndex = threadIndex;
      rwB = b;
      blindWriteAllowed = params.isAllowBlindWrites();
      sizeOfAttribute = params.getSizeOfAnAttribute();
      toDo = rwB.length;
      keyGen = keyGenerator;
   }

   @Override
   public boolean hasNext() {
      return currentOp < toDo;
   }

   @Override
   public XactOp next() {
      XactOp toRet;
      int key;
      if (!rwB[currentOp]) {  //Read
         do {
            key = r.nextInt(numKeys);
         }
         while (readSet.contains(key));  //avoid repetitions
         readSet.addLast(key);
         toRet = new XactOp(keyGen.generateKey(nodeIndex, threadIndex, key),
                            null, false);    //add a read
      } else {    //Put
         if (blindWriteAllowed) {        //You can have (distinct) blind writes
            do {
               key = r.nextInt(numKeys);
            }
            while (writeSet.contains(key));  //avoid repetitions among writes
            writeSet.add(key);
            toRet = new XactOp(keyGen.generateKey(nodeIndex, threadIndex, key),
                               generateRandomString(sizeOfAttribute), true);    //add a write op
         } else { //No blind writes: Take a value already read and increment         To have distinct writes, remember numWrites<=numReads in this case
            toRet = new XactOp(keyGen.generateKey(nodeIndex, threadIndex, readSet.get(indexNextWrite)),
                               generateRandomString(sizeOfAttribute), true);
            indexNextWrite++;
         }
      }
      currentOp++;
      return toRet;
   }


   @Override
   public void remove() {
      throw new UnsupportedOperationException("Remove is not supported");
   }

   protected final String generateRandomString(int size) {
      // each char is 2 bytes
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < size / 2; i++) sb.append((char) (64 + r.nextInt(26)));
      return sb.toString();
   }

   @Override
   public String toString() {
      return "IteratorRuntimeDap_UP{" +
            "readSet=" + readSet +
            ", writeSet=" + writeSet +
            ", params=" + params +
            ", rwB=" + Arrays.toString(rwB) +
            ", currentOp=" + currentOp +
            ", keyGen=" + keyGen +
            ", r=" + r +
            ", numKeys=" + numKeys +
            ", nodeIndex=" + nodeIndex +
            ", threadIndex=" + threadIndex +
            ", blindWriteAllowed=" + blindWriteAllowed +
            ", sizeOfAttribute=" + sizeOfAttribute +
            ", indexNextWrite=" + indexNextWrite +
            ", toDo=" + toDo +
            '}';
   }
}
