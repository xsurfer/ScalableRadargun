package org.radargun.stages;/*
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

import org.radargun.stressors.PutGetStressor;
import org.radargun.stressors.SyntheticStressor;

import java.util.Map;

/**
 * @author Diego Didona, didona@gsd.inesc-id.pt
 *         Date: 20/03/13
 */
public class SyntheticBenchmarkStage extends WebSessionBenchmarkStage {

   private int numWrites = 1;

   protected Map<String, String> doWork() {
      log.info("Starting " + getClass().getSimpleName() + ": " + this);
      SyntheticStressor putGetStressor = new SyntheticStressor();
      putGetStressor.setNodeIndex(getSlaveIndex());
      putGetStressor.setNumberOfAttributes(numberOfAttributes);
      putGetStressor.setNumOfThreads(numOfThreads);
      putGetStressor.setSizeOfAnAttribute(sizeOfAnAttribute);
      putGetStressor.setWritePercentage(writePercentage);
      putGetStressor.setKeyGeneratorClass(keyGeneratorClass);
      putGetStressor.setUseTransactions(useTransactions);
      putGetStressor.setCommitTransactions(commitTransactions);
      putGetStressor.setTransactionSize(transactionSize);
      putGetStressor.setDurationMillis(durationMillis);
      return putGetStressor.stress(cacheWrapper);
   }
}
