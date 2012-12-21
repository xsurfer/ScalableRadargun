package org.radargun.stages;

import org.radargun.state.MasterState;

import java.util.Map;

/**
 * @author Diego Didona, didona@gsd.inesc-id.pt
 *         Date: 20/12/12
 */
public class TpccCsvReportGenerationStage extends CsvReportGenerationStage {

   protected final String reportFileName(MasterState masterState) {

      Map<Integer, Map<String, Object>> results = (Map<Integer, Map<String, Object>>) masterState.get("results");
      Map<String, Object> firstResult = results.get(0);
      @SuppressWarnings("unchecked")
      int slaves = masterState.getSlavesCountForCurrentStage();
      StringBuilder sb = new StringBuilder();
      int numThreads = toInt(firstResult, "NumThreads");
      int lambda = toInt(firstResult, "Lambda");
      int thinkTime = toInt(firstResult, "ThinkTime");

      sb.append(masterState.nameOfTheCurrentBenchmark());
      sb.append("_");
      sb.append(masterState.configNameOfTheCurrentBenchmark());
      sb.append("_");
      sb.append(numThreads);
      sb.append("T_");
      sb.append(slaves);
      sb.append(".csv");
      return sb.toString();
   }

   private int toInt(Map<String, Object> results, String stat) {
      String o = (String)results.get(stat);
      return o == null ? 0 : Integer.parseInt(o);
   }
}
