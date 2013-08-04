package org.radargun.portings.tpcc;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.ITransaction;
import org.radargun.TransactionFactory;
import org.radargun.stages.stressors.producer.RequestType;
import org.radargun.stages.stressors.tpcc.TpccParameters;

/**
 * Created by: Fabio Perfetti
 * E-mail: perfabio87@gmail.com
 * Date: 8/4/13
 */
public class TpccTxFactory implements TransactionFactory {

    private static Log log = LogFactory.getLog(TpccTxFactory.class);
    private TpccTerminal terminal;
    private TpccParameters parameters;

    private int threadIndex;

    public TpccTxFactory(TpccParameters parameters, int threadIndex){
        this.parameters = parameters;
        this.threadIndex = threadIndex;

        terminal = new TpccTerminal(parameters.getPaymentWeight(), parameters.getOrderStatusWeight(), parameters.getNodeIndex(), 0);
    }

    @Override
    public int nextTransaction() {
        return terminal.chooseTransactionType(
                parameters.getCacheWrapper().isPassiveReplication(),
                parameters.getCacheWrapper().isTheMaster()
        );
    }

    @Override
    public ITransaction generateTransaction(RequestType type) {
        return terminal.createTransaction(type.getTransactionType(), threadIndex);
    }

    @Override
    public ITransaction choiceTransaction() {

        ITransaction transaction = terminal.choiceTransaction(parameters.getCacheWrapper().isPassiveReplication(), parameters.getCacheWrapper().isTheMaster(), threadIndex);
        log.info("Closed system: starting a brand new transaction of type " + transaction.getType());
        return transaction;
    }
}
