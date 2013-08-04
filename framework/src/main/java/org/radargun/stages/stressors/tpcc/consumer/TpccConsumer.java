package org.radargun.stages.stressors.tpcc.consumer;

import org.radargun.CacheWrapper;
import org.radargun.TransactionFactory;
import org.radargun.portings.tpcc.TpccTerminal;
import org.radargun.stages.AbstractBenchmarkStage;
import org.radargun.stages.stressors.AbstractBenchmarkStressor;
import org.radargun.stages.stressors.consumer.Consumer;
import org.radargun.stages.stressors.systems.SystemType;
import org.radargun.stages.stressors.tpcc.TpccParameters;

/**
 * Created by: Fabio Perfetti
 * E-mail: perfabio87@gmail.com
 * Date: 5/6/13
 */
public class TpccConsumer extends Consumer {

    private final TpccTerminal terminal;

    public TpccConsumer(int localWarehouseID,
                        int threadIndex,
                        CacheWrapper cacheWrapper,
                        SystemType system,
                        AbstractBenchmarkStage stage,
                        AbstractBenchmarkStressor stressor,
                        TpccParameters parameters,
                        TransactionFactory factory) {

        super(cacheWrapper, threadIndex, system, stage, stressor, parameters, factory);
        this.terminal = new TpccTerminal(parameters.getPaymentWeight(), parameters.getOrderStatusWeight(), parameters.getNodeIndex(), localWarehouseID);
    }

    public TpccTerminal getTerminal(){ return terminal; }

}
