package org.radargun.portings.tpcc.transaction;

import org.radargun.CacheWrapper;
import org.radargun.portings.tpcc.ElementNotFoundException;
import org.radargun.portings.tpcc.TpccTerminal;
import org.radargun.portings.tpcc.TpccTools;
import org.radargun.portings.tpcc.domain.Customer;
import org.radargun.portings.tpcc.domain.District;
import org.radargun.portings.tpcc.domain.History;
import org.radargun.portings.tpcc.domain.Warehouse;
import org.radargun.stressors.AbstractBenchmarkStressor;

import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

/**
 * @author peluso@gsd.inesc-id.pt , peluso@dis.uniroma1.it
 * @author Pedro Ruivo
 */
public class PaymentTransaction extends AbstractTpccTransaction {

    private final long terminalWarehouseID;

    private final long districtID;

    private final long customerDistrictID;

    private long customerWarehouseID;

    private final long customerID;

    private final boolean customerByName;

    private final String customerLastName;

    private final double paymentAmount;

    private final int slaveIndex;


    public PaymentTransaction(TpccTools tpccTools, int threadId, int slaveIndex, int warehouseID) {

        super(tpccTools, threadId);
        this.slaveIndex = slaveIndex;

        if (warehouseID <= 0) {
            this.terminalWarehouseID = tpccTools.randomNumber(1, TpccTools.NB_WAREHOUSES);
        } else {
            this.terminalWarehouseID = warehouseID;
        }

        this.districtID = tpccTools.randomNumber(1, TpccTools.NB_MAX_DISTRICT);

        long x = tpccTools.randomNumber(1, 100);

        if (x <= 85) {
            this.customerDistrictID = this.districtID;
            this.customerWarehouseID = this.terminalWarehouseID;
        } else {
            this.customerDistrictID = tpccTools.randomNumber(1, TpccTools.NB_MAX_DISTRICT);
            do {
                this.customerWarehouseID = tpccTools.randomNumber(1, TpccTools.NB_WAREHOUSES);
            }
            while (this.customerWarehouseID == this.terminalWarehouseID && TpccTools.NB_WAREHOUSES > 1);
        }

        long y = tpccTools.randomNumber(1, 100);

        if (y <= 60) {
            this.customerByName = true;

            customerLastName = lastName((int) tpccTools.nonUniformRandom(TpccTools.C_C_LAST, TpccTools.A_C_LAST, 0, TpccTools.MAX_C_LAST));
            this.customerID = -1;
        } else {
            this.customerByName = false;
            this.customerID = tpccTools.nonUniformRandom(TpccTools.C_C_ID, TpccTools.A_C_ID, 1, TpccTools.NB_MAX_CUSTOMER);
            this.customerLastName = null;
        }

        this.paymentAmount = tpccTools.randomNumber(100, 500000) / 100.0;


    }

    @Override
    public void executeTransaction(CacheWrapper cacheWrapper) throws Throwable {
        paymentTransaction(cacheWrapper);
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }


    private void paymentTransaction(CacheWrapper cacheWrapper) throws Throwable {
        String w_name;
        String d_name;
        long nameCnt;

        String new_c_last;

        String c_data, c_new_data, h_data;


        Warehouse w = new Warehouse();
        w.setW_id(terminalWarehouseID);

        boolean found = w.load(cacheWrapper);
        if (!found) throw new ElementNotFoundException("W_ID=" + terminalWarehouseID + " not found!");
        w.setW_ytd(paymentAmount);
        w.threadAwareStore(cacheWrapper, threadId);


        District d = new District();
        d.setD_id(districtID);
        d.setD_w_id(terminalWarehouseID);
        found = d.load(cacheWrapper);
        if (!found)
            throw new ElementNotFoundException("D_ID=" + districtID + " D_W_ID=" + terminalWarehouseID + " not found!");

        d.setD_ytd(paymentAmount);
        d.threadAwareStore(cacheWrapper, threadId);


        Customer c = null;

        if (customerByName) {

            List<Customer> cList = customerList(cacheWrapper, customerWarehouseID, customerDistrictID, customerLastName);

            if ((cList == null || cList.isEmpty()))
                throw new ElementNotFoundException("C_LAST=" + customerLastName + " C_D_ID=" + customerDistrictID + " C_W_ID=" + customerWarehouseID + " not found!");

            Collections.sort(cList);

            nameCnt = cList.size();

            if (nameCnt % 2 == 1) nameCnt++;
            Iterator<Customer> itr = cList.iterator();

            for (int i = 1; i <= nameCnt / 2; i++) {
                c = itr.next();
            }
        } else {

            c = new Customer();
            c.setC_id(customerID);
            c.setC_d_id(customerDistrictID);
            c.setC_w_id(customerWarehouseID);
            found = c.load(cacheWrapper);
            if (!found)
                throw new ElementNotFoundException("C_ID=" + customerID + " C_D_ID=" + customerDistrictID + " C_W_ID=" + customerWarehouseID + " not found!");


        }


        c.setC_balance(c.getC_balance() + paymentAmount);
        if (c.getC_credit().equals("BC")) {

            c_data = c.getC_data();

            c_new_data = c.getC_id() + " " + customerDistrictID + " " + customerWarehouseID + " " + districtID + " " + terminalWarehouseID + " " + paymentAmount + " |";
            if (c_data.length() > c_new_data.length()) {
                c_new_data += c_data.substring(0, c_data.length() - c_new_data.length());
            } else {
                c_new_data += c_data;
            }

            if (c_new_data.length() > 500) c_new_data = c_new_data.substring(0, 500);

            c.setC_data(c_new_data);

            c.threadAwareStore(cacheWrapper, threadId);


        } else {
            c.threadAwareStore(cacheWrapper, threadId);

        }

        w_name = w.getW_name();
        d_name = d.getD_name();

        if (w_name.length() > 10) w_name = w_name.substring(0, 10);
        if (d_name.length() > 10) d_name = d_name.substring(0, 10);
        h_data = w_name + "    " + d_name;

        History h = new History(c.getC_id(), customerDistrictID, customerWarehouseID, districtID, terminalWarehouseID, new Date(), paymentAmount, h_data);
        h.threadAwareStore(cacheWrapper, this.slaveIndex, threadId);


    }

    @Override
    public int getType() {
        return TpccTerminal.PAYMENT;
    }
}
