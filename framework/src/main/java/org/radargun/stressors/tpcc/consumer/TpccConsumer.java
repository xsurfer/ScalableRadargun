package org.radargun.stressors.tpcc.consumer;

import org.radargun.CacheWrapper;
import org.radargun.portings.tpcc.TpccTerminal;
import org.radargun.stages.AbstractBenchmarkStage;
import org.radargun.stressors.BenchmarkStressor;
import org.radargun.stressors.consumer.Consumer;
import org.radargun.stressors.tpcc.TpccStressorParameter;
import org.radargun.workloadGenerator.SystemType;

/**
 * Created by: Fabio Perfetti
 * E-mail: perfabio87@gmail.com
 * Date: 5/6/13
 */
public class TpccConsumer extends Consumer {

    private final TpccTerminal terminal;

    public TpccConsumer(int localWarehouseID, int threadIndex, CacheWrapper cacheWrapper, SystemType system, AbstractBenchmarkStage stage, BenchmarkStressor stressor, TpccStressorParameter parameters) {
        super(cacheWrapper, threadIndex, system, stage, stressor, parameters);
        this.terminal = new TpccTerminal(parameters.getPaymentWeight(), parameters.getOrderStatusWeight(), parameters.getNodeIndex(), localWarehouseID);
    }

    public TpccTerminal getTerminal(){ return terminal; }

}
