package org.radargun.reporting;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.title.TextTitle;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
import org.radargun.utils.Utils;

import java.awt.*;
import java.io.File;


/**
 * Created with IntelliJ IDEA. User: fabio Date: 12/6/12 Time: 8:05 PM To change this template use File | Settings |
 * File Templates.
 */
public class StepClusterReport implements ClusterReport {

   private static Log log = LogFactory.getLog(StepClusterReport.class);

   private XYSeriesCollection categorySet = new XYSeriesCollection();
   private final XYSeries s1 = new XYSeries("History", true, true);
   private String reportDir;
   private String fileName;
   private String xLabel;
   private String yLabel;
   private String title;
   private String subtitle;


   public void setReportFile(String reportDir, String fileName) {
      this.reportDir = reportDir;
      this.fileName = fileName;
   }

   public void init(String xLabels, String yLabels, String title, String subtitle) {
      this.xLabel = xLabels;
      this.yLabel = yLabels;
      this.title = title;
      this.subtitle = subtitle;
   }

   @Override
   public void addCategory(String productName, int clusterSize, Number value) {
      //To change body of implemented methods use File | Settings | File Templates.
   }


   public void addCategory(String productName, double time, Number value) {
      s1.add(time, value);

   }

   public void generate() throws Exception {
//        sort();
      File root = new File(reportDir);
      if (!root.exists()) {
         if (root.mkdirs()) {
            log.warn("Could not create root dir : " + root.getAbsolutePath() + " This might result in reports not being generated");
         } else {
            log.info("Created root file: " + root);
         }
      }
      File chartFile = new File(root, fileName + ".png");
      Utils.backupFile(chartFile);

      ChartUtilities.saveChartAsPNG(chartFile, createChart(), 1024, 768);

      log.info("Chart saved as " + chartFile);
   }

//    /**
//     * Crappy that the JFeeChart data set doesn't order columns and rows by default or even as an option.  Need to do
//     * this manually.
//     */
//    private void sort() {
//        SortedMap<Comparable, SortedMap<Comparable, Number>> raw = new TreeMap<Comparable, SortedMap<Comparable, Number>>();
//        for (int i = 0; i < categorySet.getRowCount(); i++) {
//            Comparable row = categorySet.getRowKey(i);
//            SortedMap<Comparable, Number> rowData = new TreeMap<Comparable, Number>();
//            for (int j = 0; j < categorySet.getColumnCount(); j++) {
//                Comparable column = categorySet.getColumnKey(j);
//                Number value = categorySet.getValue(i, j);
//                rowData.put(column, value);
//            }
//            raw.put(row, rowData);
//        }
//
//        categorySet.clear();
//        for (Comparable row : raw.keySet()) {
//            Map<Comparable, Number> rowData = raw.get(row);
//            for (Comparable column : rowData.keySet()) {
//                categorySet.addValue(rowData.get(column), row, column);
//            }
//        }
//    }

   private JFreeChart createChart() {
      JFreeChart chart = ChartFactory.createXYStepChart(title, xLabel, yLabel, categorySet, PlotOrientation.VERTICAL, true, false, false);
      chart.addSubtitle(new TextTitle(subtitle));
      chart.setBorderVisible(true);
      chart.setAntiAlias(true);
      chart.setTextAntiAlias(true);
      chart.setBackgroundPaint(new Color(0x61, 0x9e, 0xa1));
      chart.getXYPlot().setDomainAxis(new NumberAxis());
      categorySet.addSeries(s1);
      return chart;
   }
}
