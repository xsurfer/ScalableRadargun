package org.radargun.portings.tpcc.transaction;

import org.radargun.CacheWrapper;
import org.radargun.Transaction;
import org.radargun.portings.tpcc.TpccTerminal;
import org.radargun.portings.tpcc.TpccTools;
import org.radargun.portings.tpcc.dac.CustomerDAC;
import org.radargun.portings.tpcc.domain.Customer;
import org.radargun.stressors.AbstractBenchmarkStressor;

import java.util.List;

/**
 * @author Diego Didona, didona@gsd.inesc-id.pt
 *         Date: 18/12/12
 */
public abstract class AbstractTpccTransaction extends Transaction {

   private static boolean avoidNotFoundExceptions = true;
   protected TpccTools tpccTools;
   protected int threadId;

   public static void setAvoidNotFoundExceptions(boolean b) {
      avoidNotFoundExceptions = b;
   }

   protected boolean isAvoidNotFoundExceptions() {
      return avoidNotFoundExceptions;
   }

   public AbstractTpccTransaction(TpccTools tools, int threadId) {
       this.tpccTools = tools;
      this.threadId = threadId;
   }

   protected List<Customer> customerList(CacheWrapper cacheWrapper, long warehouseID, long districtID, String customerLastName) throws Throwable {
      String new_c_last = customerLastName;
      List<Customer> cList;
      do {
         cList = CustomerDAC.loadByCLast(cacheWrapper, warehouseID, districtID, new_c_last);
      }
      while (isAvoidNotFoundExceptions() && (cList == null || cList.isEmpty())
              && (new_c_last = lastName((int) tpccTools.nonUniformRandom(TpccTools.C_C_LAST, TpccTools.A_C_LAST, 0, TpccTools.MAX_C_LAST))) != null);
      return cList;
   }

   protected String lastName(int num) {
        return TpccTerminal.nameTokens[num / 100] + TpccTerminal.nameTokens[(num / 10) % 10] + TpccTerminal.nameTokens[num % 10];
     }

}
