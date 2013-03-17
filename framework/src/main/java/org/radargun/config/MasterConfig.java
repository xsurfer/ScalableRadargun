package org.radargun.config;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.ScalingMaster;

/**
 * Comntains master's configuration elements.
 *
 * @author Mircea.Markus@jboss.com
 */
public class MasterConfig {

   private static Log log = LogFactory.getLog(MasterConfig.class);

   private int port;
   private String host;
   private int slavesCount;

    /**
     * This list contains as many ScalingBenchmarkConfig as the product (PRODUCTS * CONFIGURATION FILE)
     */
   List<FixedSizeBenchmarkConfig> benchmarks = new ArrayList<FixedSizeBenchmarkConfig>();

   public MasterConfig(int port, String host, int slavesCount) {
      this.port = port;
      this.host = host;
      this.slavesCount = slavesCount;
   }

   public int getPort() {
      return port;
   }

   public String getHost() {
      return host;
   }

   public int getSlaveCount() {
      return slavesCount;
   }

    /** questo metodo viene chiamato quando si aggiungono/rimuovono nuovi thread a runtime
     *  ed è incaricato di aggiornare anche il numero di slaves nello stage attualmente in esecuzione
     *  che DEVE essere OBLiGATORIAMENTE il WebSessionBenchmarkStage!!!
     **/
//    @Deprecated
//    public void setSlavesCount(int slavesCount){
//        this.slavesCount = slavesCount;
//        log.debug("Modifico numero di slaves in CurrentDistStage:" + ScalingMaster.getInstance(null).state.getCurrentDistStage().getClass().getName() + "(deve essere WebSessionBenchmark!!)" );
//        for (FixedSizeBenchmarkConfig f : benchmarks) {
//            f.setSize(slavesCount);
//        }
//
//    }

   public List<FixedSizeBenchmarkConfig> getBenchmarks() {
      return benchmarks;
   }

   public void addBenchmark(FixedSizeBenchmarkConfig config) {
      benchmarks.add(config);
   }

   public void validate() {
      Set<String> allBenchmarkNames = new HashSet<String>();
      for (FixedSizeBenchmarkConfig f : benchmarks) {
         if (!allBenchmarkNames.add(f.getProductName())) {
            throw new RuntimeException("There are two benchmarks having same name:" + f.getProductName() + ". Benchmark name should be unique!");
         }
      }
   }
}
