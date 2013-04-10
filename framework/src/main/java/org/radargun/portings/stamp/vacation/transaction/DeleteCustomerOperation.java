package org.radargun.portings.stamp.vacation.transaction;

import org.radargun.CacheWrapper;
import org.radargun.Transaction;
import org.radargun.portings.stamp.vacation.Random;
import org.radargun.portings.stamp.vacation.domain.Manager;

public class DeleteCustomerOperation implements Transaction {

    final private int customerId;

    public DeleteCustomerOperation(Random randomPtr, int queryRange, int relations) {
	this.customerId = randomPtr.posrandom_generate() % relations;
    }

    @Override
    public void executeTransaction(CacheWrapper cache) throws Throwable {
	Manager managerPtr = (Manager) cache.get(null, "MANAGER");
	int bill = managerPtr.manager_queryCustomerBill(cache, customerId);
	if (bill >= 0) {
	    managerPtr.manager_deleteCustomer(cache, customerId);
	}
    }

    @Override
    public boolean isReadOnly() {
	return false;
    }

    @Override
    public int getType() {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

}
