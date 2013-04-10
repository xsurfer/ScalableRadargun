package org.radargun.stages.tpcc;

import org.radargun.stages.CsvReportGenerationStage;
import org.radargun.state.MasterState;

import java.util.Map;

/**
 * @author Diego Didona, didona@gsd.inesc-id.pt
 *         Date: 20/12/12
 */
public class TpccCsvReportGenerationStage extends CsvReportGenerationStage {
   /*
   We could pass the list of attributes to query (and the string to identify them) via xml
   but I'm sleepy and papa wants its test donw by tomorrow morning =)
    */
   protected final String reportFileName(MasterState masterState) {

      Map<Integer, Map<String, Object>> results = (Map<Integer, Map<String, Object>>) masterState.get("results");
      Map<String, Object> firstResult = results.get(0);
      return masterState.nameOfTheCurrentBenchmark() + "_" + masterState.configNameOfTheCurrentBenchmark() + "_" + masterState.getSlavesCountForCurrentStage() +"_"+firstResult.get("TEST_ID")+".csv";
   }

}
