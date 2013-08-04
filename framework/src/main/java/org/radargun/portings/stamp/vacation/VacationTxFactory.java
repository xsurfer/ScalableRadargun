package org.radargun.portings.stamp.vacation;

import org.radargun.ITransaction;
import org.radargun.TransactionFactory;
import org.radargun.portings.stamp.vacation.transaction.DeleteCustomerOperation;
import org.radargun.portings.stamp.vacation.transaction.MakeReservationOperation;
import org.radargun.portings.stamp.vacation.transaction.UpdateTablesOperation;
import org.radargun.stages.stressors.producer.RequestType;
import org.radargun.stages.stressors.stamp.vacation.VacationParameter;

/**
 * Author: Fabio Perfetti (perfabio87 [at] gmail.com)
 * Date: 8/3/13
 * Time: 6:50 PM
 */
public class VacationTxFactory implements TransactionFactory {

    private Random rnd = new Random();
    private VacationParameter parameters;

    public VacationTxFactory(VacationParameter parameters){
        this.parameters = parameters;
    }

    @Override
    public int nextTransaction() {
        int r = rnd.posrandom_generate() % 100;
        int action = selectAction(r, parameters.getPercentUser());
        //RequestType requestType = new RequestType(System.nanoTime(),action);

        return action;
    }

    @Override
    public ITransaction generateTransaction(RequestType type) {

        int action = type.getTransactionType();
        ITransaction result = null;

        if (action == Definitions.ACTION_MAKE_RESERVATION) {
            result = new MakeReservationOperation(rnd, parameters.getQueryPerTx(), parameters.getQueryRange(), parameters.getRelations(), parameters.getReadOnlyPerc());
        } else if (action == Definitions.ACTION_DELETE_CUSTOMER) {
            result = new DeleteCustomerOperation(rnd, parameters.getQueryRange(), parameters.getRelations());
        } else if (action == Definitions.ACTION_UPDATE_TABLES) {
            result = new UpdateTablesOperation(rnd, parameters.getQueryPerTx(), parameters.getQueryRange(), parameters.getRelations());
        } else {
            assert (false);
        }

        return result;
    }

    @Override
    public ITransaction choiceTransaction() {
        int r = rnd.posrandom_generate() % 100;
        int action = selectAction(r, parameters.getPercentUser());
        RequestType requestType = new RequestType(System.nanoTime(),action);

        return generateTransaction(requestType);
    }

    private int selectAction(int r, int percentUser) {
        if (r < percentUser) {
            return Definitions.ACTION_MAKE_RESERVATION;
        } else if ((r & 1) == 1) {
            return Definitions.ACTION_DELETE_CUSTOMER;
        } else {
            return Definitions.ACTION_UPDATE_TABLES;
        }
    }

}
