package org.radargun.stages;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.reporting.ClusterReport;
import org.radargun.reporting.LineClusterReport;
import org.radargun.reporting.StepClusterReport;
import org.radargun.utils.Utils;

import java.io.*;
import java.util.*;
import java.util.regex.Pattern;

/**
 * Created with IntelliJ IDEA.
 * User: fabio
 * Date: 12/6/12
 * Time: 2:05 PM
 * To change this template use File | Settings | File Templates.
 */
public class GenerateScalingChartStage extends GenerateChartStage {

    private static Log log = LogFactory.getLog(GenerateChartStage.class);

    public static final String X_LABEL = "Cluster size (number of cache instances)";

    ClusterReport historyReport = new StepClusterReport();

    public boolean execute() throws Exception {

        historyReport.setReportFile(reportDirectory, fnPrefix + "_HISTORY_CLUSTER");
        historyReport.init("TIME (nsec)", "CLUSTER SIZE (number of cache instances) ", "History of evolution ", getSubtitle());

        File[] files = getFilteredFiles(new File(csvFilesDirectory));
        for (File f : files) {
            readData(f);
        }

        historyReport.generate();
        return true;
    }

    private void readData(File f) throws IOException {
        log.debug("Processing file " + f);
        //expected file format is: <product>_<config>_<size>.csv
        String productName = null;
        String configName = null;
        int clusterSize = 0;
        try {
            StringTokenizer tokenizer = new StringTokenizer(Utils.fileName2Config(f.getName()), "_");
            productName = tokenizer.nextToken();
            configName = tokenizer.nextToken();
            clusterSize = Integer.parseInt(tokenizer.nextToken());
        } catch (Throwable e) {
            String fileName = f == null ? null : f.getAbsolutePath();
            log.error("unexpected exception while parsing filename: " + fileName, e);
        }

        String line;
        BufferedReader br = new BufferedReader(new FileReader(f));

        TreeSet<Intervall> durationSet = new TreeSet<Intervall>(new Comparatore());
        TreeSet<Intervall> intervalConflictSet = new TreeSet<Intervall>(new Comparatore());
        TreeSet<Intervall> noConflictSet = new TreeSet<Intervall>(new Comparatore());

        Intervall st = null;
        br.readLine(); // header
        while ((line = br.readLine()) != null) {
            st = getSlaveTime(line);
            durationSet.add(st);
            intervalConflictSet.add(st);
            log.debug("Read stats " + st);
        }
        br.close();


        while (!intervalConflictSet.isEmpty()) {
            Intervall first = intervalConflictSet.pollFirst();

            if (intervalConflictSet.isEmpty())
                break;
            Intervall second = intervalConflictSet.first();

            if (Intervall.isConflict(first, second)) {
                intervalConflictSet.addAll(solveConflict(first, second));
            } else {
                noConflictSet.add(first);
            }
        }

        slavesPerIntervall(noConflictSet, durationSet);

        String name = productName + "(" + configName + ")";

        Iterator<Intervall> iter = noConflictSet.iterator();
        while (iter.hasNext()) {
            Intervall next = iter.next();
            historyReport.addCategory(name, next.start, next.slaves);
            //historyReport.addCategory(name, next.end, next.slaves);
        }

    }

    private void slavesPerIntervall(TreeSet<Intervall> noConflict, TreeSet<Intervall> durationSet) {
        Iterator<Intervall> iter = noConflict.iterator();
        while (iter.hasNext()) {
            Intervall next = iter.next();
            int slaves = 0;
            for (Intervall orig : durationSet){
                if(next.containedIn(orig))
                    slaves++;
            }
            next.slaves = slaves;
        }
    }



    private TreeSet<Intervall> solveConflict(Intervall first, Intervall second) {
        TreeSet<Intervall> tmpIntSet = new TreeSet<Intervall>();

        // se il secondo inizia prima che il prima: scambio
        if (second.start < first.start) {
            Intervall tmp = first;
            first = second;
            second = tmp;
        }

        Intervall intervall = new Intervall();
        intervall.start = first.start;
        intervall.end = second.start;
        tmpIntSet.add(intervall);

        intervall = new Intervall();
        // secondo contenuto nel primo
        if (second.end < first.end) {
            intervall.start = second.start;
            intervall.end = second.end;
            tmpIntSet.add(intervall);

            intervall = new Intervall();
            intervall.start = second.end;
            intervall.end = first.end;
            tmpIntSet.add(intervall);

        } else {
            intervall.start = second.start;
            intervall.end = first.end;
            tmpIntSet.add(intervall);

            intervall = new Intervall();
            intervall.start = first.end;
            intervall.end = second.end;
            tmpIntSet.add(intervall);
        }
        return tmpIntSet;
    }

    private Intervall getSlaveTime(String line) {
        // To be a valid line, the line should be comma delimited
        StringTokenizer strTokenizer = new StringTokenizer(line, ",");
        if (strTokenizer.countTokens() < 7) return null;

        strTokenizer.nextToken();//skip index

        String durationStr = strTokenizer.nextToken(); //this is duration
        String initTimeStr = "vuoto";
        for(int i=0; i<26; i++){
            initTimeStr = strTokenizer.nextToken();    //this is initTime
        }

        log.debug("DURATION è: " + durationStr);
        log.debug("INIT TIME è: " + initTimeStr);

        Intervall s = new Intervall();
        try {
            s.start = Double.parseDouble(initTimeStr);
            s.end = s.start + Double.parseDouble(durationStr);
        } catch (NumberFormatException nfe) {
            log.error("Unable to parse file properly!", nfe);
            return null;
        }
        return s;
    }

    private static class Intervall implements Comparable {
        private double start, end, slaves;

        public String toString() {
            return "Intervall{" +
                    "start=" + start +
                    ", end=" + end +
                    ", slaves=" + slaves +
                    '}';
        }

        public boolean containedIn(Intervall b){
            if( (this.start >= b.start) && (this.end <= b.end) )
                return true;
            return false;
        }

        private static boolean isConflict(Intervall first, Intervall second) {
            // se il secondo inizia prima che il prima: scambio
            if (second.start < first.start) {
                Intervall tmp = first;
                first = second;
                second = tmp;
            }
            if (second.start < first.end)
                return true;
            return false;
        }

        @Override
        public int compareTo(Object o) {
            if (this.start < ((Intervall) o).start )
                return -1;
            else if (this.start > ((Intervall) o).start )
                return 1;
            else
                return 0;
        }
    }

    private class Comparatore implements Comparator<Intervall> {
        public int compare(Intervall o1, Intervall o2) {
            return o1.compareTo(o2);
        }
    }


}
