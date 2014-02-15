package org.radargun.stages.stressors.tpcc;

import org.radargun.CacheWrapper;
import org.radargun.stages.stressors.Parameters;
import org.radargun.stages.synthetic.XACT_RETRY;

/**
 * Created by: Fabio Perfetti E-mail: perfabio87@gmail.com Date: 5/6/13
 */
public class TpccParameters extends Parameters {

   /**
    * percentage of Payment transactions
    */
   private int paymentWeight = 45;

   /**
    * percentage of Order Status transactions
    */
   private int orderStatusWeight = 5;

   /**
    * specify the min and the max number of items created by a New Order Transaction. format: min,max
    */
   private final String numberOfItemsInterval;

   /**
    * if true, each node will pick a warehouse and all transactions will work over that warehouse. The warehouses are
    * picked by order, i.e., slave 0 gets warehouse 1,N+1, 2N+1,[...]; ... slave N-1 gets warehouse N, 2N, [...].
    */
   private final boolean accessSameWarehouse;


   public TpccParameters(CacheWrapper cacheWrapper,
                         long simulationTimeSec,
                         int numOfThreads,
                         int nodeIndex,
                         long backOffTime,
                         XACT_RETRY retryOnAbort,
                         long statsSamplingInterval,

                         int paymentWeight,
                         int orderStatusWeight,
                         String numberOfItemsInterval,
                         boolean accessSameWarehouse
   ) {
      super(cacheWrapper, simulationTimeSec, numOfThreads, nodeIndex, backOffTime, retryOnAbort, statsSamplingInterval);
      this.paymentWeight = paymentWeight;
      this.orderStatusWeight = orderStatusWeight;
      this.numberOfItemsInterval = numberOfItemsInterval;
      this.accessSameWarehouse = accessSameWarehouse;
   }


   public int getPaymentWeight() {
      return paymentWeight;
   }

   public void setPaymentWeight(int paymentWeight) {
      this.paymentWeight = paymentWeight;
   }

   public int getOrderStatusWeight() {
      return orderStatusWeight;
   }

   public void setOrderStatusWeight(int orderStatusWeight) {
      this.orderStatusWeight = orderStatusWeight;
   }

   public boolean isAccessSameWarehouse() {
      return accessSameWarehouse;
   }
   //public void setAccessSameWarehouse(boolean accessSameWarehouse) { this.accessSameWarehouse = accessSameWarehouse; }

   public String getNumberOfItemsInterval() {
      return numberOfItemsInterval;
   }
   //public void setNumberOfItemsInterval(String numberOfItemsInterval) { this.numberOfItemsInterval = numberOfItemsInterval; }
}
