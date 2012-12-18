package org.radargun.tpcc.transaction;

import org.radargun.CacheWrapper;
import org.radargun.tpcc.ElementNotFoundException;
import org.radargun.tpcc.TpccTerminal;
import org.radargun.tpcc.TpccTools;
import org.radargun.tpcc.dac.CustomerDAC;
import org.radargun.tpcc.domain.Customer;

import java.util.List;

/**
 * @author Diego Didona, didona@gsd.inesc-id.pt
 *         Date: 18/12/12
 */
public abstract class AbstractTpccTransaction implements TpccTransaction {

   private static boolean avoidNotFoundExceptions = true;
   protected TpccTools tpccTools;

   public static void setAvoidNotFoundExceptions(boolean b) {
      avoidNotFoundExceptions = b;
   }

   protected boolean isAvoidNotFoundExceptions() {
      return avoidNotFoundExceptions;
   }

   public AbstractTpccTransaction(TpccTools tools, int... ints) {
      this.tpccTools = tools;
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
