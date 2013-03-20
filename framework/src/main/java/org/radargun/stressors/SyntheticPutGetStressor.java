package org.radargun.stressors;/*
 * INESC-ID, Instituto de Engenharia de Sistemas e Computadores Investigação e Desevolvimento em Lisboa
 * Copyright 2013 INESC-ID and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 3.0 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

/**
 * @author Diego Didona, didona@gsd.inesc-id.pt
 *         Date: 20/03/13
 */
public class SyntheticPutGetStressor extends PutGetStressor {
   private int readOnlyXactSize = 1;
   private int updateXactWrites = 1;
   private int updateXactReads = 1;


   public int getupdateXactWrites() {
      return updateXactWrites;
   }

   public void setupdateXactWrites(int numWrites) {
      this.updateXactWrites = numWrites;
   }

   @Override
   protected List<Stressor> executeOperations() throws Exception {
      List<Stressor> stressors = new ArrayList<Stressor>(numOfThreads);
      startPoint = new CountDownLatch(1);
      for (int threadIndex = 0; threadIndex < numOfThreads; threadIndex++) {
         Stressor stressor = new Stressor(threadIndex);
         stressors.add(stressor);
         stressor.start();
      }
      log.info("Cache wrapper info is: " + cacheWrapper.getInfo());
      startPoint.countDown();
      log.info("Started " + stressors.size() + " stressor threads.");
      for (Stressor stressor : stressors) {
         stressor.join();
      }
      return stressors;
   }

   private class SyntheticStressor extends Thread {


      private KeyGenerator keyGen;
      private int nodeIndex, threadIndex, numKeys;

      SyntheticStressor(KeyGenerator keyGen, int nodeIndex, int threadIndex, int numKeys) {
         this.keyGen = keyGen;
         this.nodeIndex = nodeIndex;
         this.threadIndex = threadIndex;
         this.numKeys = numKeys;
      }

      @Override
      public void run() {
         try {
            runInternal();
         } catch (Exception e) {
            log.error("Unexpected error in stressor!", e);
         }
      }

      private void runInternal() {
         Random r = new Random();
         xactClass lastClazz;
         try {
            startPoint.await();
            log.trace("Starting thread: " + getName());
         } catch (InterruptedException e) {
            log.warn(e);
         }

         while (completion.moreToRun()) {
            try {
               lastClazz = xactClass(r);
               doXact(r,lastClazz);
            } catch (Exception e) {
               log.warn("Unexpected exception" + e.getMessage());
            }
         }
      }

      private boolean readOnlyXact(Random r, int writePerc) {
         return !cacheWrapper.isTheMaster() || r.nextInt(100) > writePerc;
      }


      private void doReadXact(Random r) throws Exception {
         int doneRead = 0;
         while (doneRead++ < readOnlyXactSize) {
            doOp(false, r.nextInt(numKeys));
         }
      }

      private void doWriteXact(Random r) throws Exception {
         int toDoRead = updateXactReads, toDoWrite = updateXactWrites, toDo = updateXactWrites + updateXactReads, writePerc = 100 * (int) (((double) updateXactWrites) / ((double) (toDo)));
         boolean doPut;
         while (toDo > 0) {
            if (toDo == toDoWrite)      //I have only puts left
               doPut = true;
            else if (toDo == toDoRead)  //I have only reads left
               doPut = false;
            else  //I choose uniformly
               doPut = r.nextInt(100) < writePerc;
            doOp(doPut, r.nextInt(numKeys));
            toDo--;
         }

      }

      private void doOp(boolean put, int keyIndex) throws Exception {
         if (put)
            cacheWrapper.put(null, keyGen.generateKey(nodeIndex, threadIndex, keyIndex), generateRandomString(sizeOfValue));
         else
            cacheWrapper.get(null, keyGen.generateKey(nodeIndex, threadIndex, keyIndex));
      }


      private xactClass xactClass(Random r) {
         if (readOnlyXact(r, writePercentage))
            return xactClass.RO;
         else return xactClass.WR;
      }


      private void doXact(Random r, xactClass clazz) throws Exception {
         cacheWrapper.startTransaction();
         try {
            switch (clazz) {
               case RO: {
                  doReadXact(r);
               }
               case WR: {
                  doWriteXact(r);
               }
               default:
                  throw new RuntimeException("Invalid xact clazz " + clazz);

            }
         } catch (Exception e) {
            log.trace("Rollback while running locally");
            cacheWrapper.endTransaction(false);
         }

         try {
            cacheWrapper.endTransaction(true);
         } catch (Exception e) {
            log.trace("Rollback at prepare time");
         }
      }


   }


   private enum xactClass {
      RO, WR
   }


}
