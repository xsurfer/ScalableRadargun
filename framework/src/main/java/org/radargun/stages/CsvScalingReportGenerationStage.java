package org.radargun.stages;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.utils.CacheSizeValues;
import org.radargun.utils.Utils;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: fabio
 * Date: 12/6/12
 * Time: 1:51 PM
 * To change this template use File | Settings | File Templates.
 */
public class CsvScalingReportGenerationStage extends CsvReportGenerationStage {

    private static Log log = LogFactory.getLog(CsvScalingReportGenerationStage.class);

    public boolean execute() {
        Map<Integer, Map<String, Object>> results = (Map<Integer, Map<String, Object>>) masterState.get("results");
        Map<Integer, Long> timeStampResults = (Map<Integer, Long>) masterState.get("timeStamps");

        if (results == null) {
            log.error("Could not find reports('results') on the master. Master's state is  " + masterState);
            return false;
        }
        if(timeStampResults == null) {
            log.error("Could not find reports('timeStampResults') on the master. Master's state is  " + masterState);
            return false;
        }

        try {
            if (results.size() == 0) {
                log.warn("Nothing to report ('results')!");
                return false;
            }
            if (timeStampResults.size() == 0) {
                log.warn("Nothing to report ('timeStampResults')!");
                return false;
            }

            joinCacheSizes(results, (List<CacheSizeValues>) masterState.get("CacheSizeResults"));
            joinInitialTs(results, timeStampResults);

            prepareOutputFile(results.size());
            writeData(results);
        } catch (Exception e) {
            log.error(e);
            return false;
        }
        return true;
    }

    protected void joinInitialTs(Map<Integer, Map<String, Object>> results, Map<Integer, Long> initialTsValues) {
        if (initialTsValues == null || initialTsValues.size() == 0) {
            log.info("Initial timestamp values not collected. Skip join to results");
            return;
        }

        for (Map.Entry<Integer, Map<String, Object>> entry : results.entrySet()) {
            int slaveIdx = entry.getKey();

            Map<String, Object> slaveResults = entry.getValue();

            try {
                slaveResults.put("INITIAL_TIMESTAMP", initialTsValues.get(slaveIdx));

            } catch (Exception e) {
                log.warn("Exception occurs while join the cache size to results for slave index " + slaveIdx, e);
            }
        }
    }

}
