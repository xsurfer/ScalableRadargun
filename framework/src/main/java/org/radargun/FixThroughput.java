package org.radargun;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 1.1
 */
public class FixThroughput implements Runnable {

   private static enum Header {
      OBSERVATIONS("Observations"),
      THROUGHPUT("Throughput"),
      WRITE_THROUGHPUT("WriteTxThroughput"),
      READ_THROUGHPUT("ReadTxThroughput"),
      EXPECTED_WRITE_PERCENTAGE("ExpectedWritePercentage");

      private final String headerName;

      Header(String headerName) {
         this.headerName = headerName;
      }

      public static Header fromString(String headerName) {
         for (Header header : values()) {
            if (header.headerName.equals(headerName)) {
               return header;
            }
         }
         return null;
      }

      @Override
      public String toString() {
         return headerName;
      }
   }

   private final String filePath;
   private final String outputFilePath;
   private final Map<Header, Integer> headerPosition;

   public FixThroughput(String filePath) {
      this.filePath = filePath;
      outputFilePath = computeOutputFilePath(filePath);
      headerPosition = new EnumMap<Header, Integer>(Header.class);
   }

   public static void main(String[] args) {
      System.out.println("Fixing " + Arrays.asList(args));

      for (String filePath : args) {
         new FixThroughput(filePath).run();
      }

      System.out.println("Finished");
      System.exit(0);
   }

   @Override
   public void run() {
      BufferedReader reader = getBufferedReader();
      BufferedWriter writer = getBufferedWriter();

      if (reader == null) {
         System.err.println("null reader");
         System.exit(1);
      }

      if (writer == null) {
         System.err.println("null writer");
         System.exit(1);
      }

      String[] line = readLine(reader);
      if (line == null) {
         System.err.println("nothing to read");
         System.exit(2);
      }

      setHeaderPosition(line);
      if (!isAllHeadersPositionValid()) {
         System.err.println("some headers are missing");
         List<Header> missing = new LinkedList<Header> (Arrays.asList(Header.values()));
         missing.removeAll(headerPosition.keySet());
         System.err.println("missing headers are " + missing);
         System.exit(3);
      }

      writeLine(line, writer);

      while ((line = readLine(reader)) != null) {
         double throughput = fixThroughput(line);
         line[headerPosition.get(Header.THROUGHPUT)] = Double.toString(throughput);
         writeLine(line, writer);
      }

      close(reader);
      close(writer);
   }

   private BufferedReader getBufferedReader() {
      try {
         return new BufferedReader(new FileReader(filePath));
      } catch (Exception e) {
         e.printStackTrace();
      }
      return null;
   }

   private BufferedWriter getBufferedWriter() {
      try {
         return new BufferedWriter(new FileWriter(outputFilePath));
      } catch (Exception e) {
         e.printStackTrace();
      }
      return null;
   }

   private void close(Closeable closeable) {
      try {
         closeable.close();
      } catch (Exception e) {
         e.printStackTrace();
      }
   }

   private String[] readLine(BufferedReader reader) {
      String line = null;
      try {
         line = reader.readLine();
      } catch (Exception e) {
         e.printStackTrace();
      }
      return line == null ? null :line.split(",");
   }

   private void writeLine(String[] line, BufferedWriter writer) {
      try {
         writer.write(line[0]);
         for (int i = 1; i < line.length; ++i) {
            writer.write(",");
            writer.write(line[i]);
         }
         writer.newLine();
         writer.flush();
      } catch (Exception e) {
         e.printStackTrace();
      }
   }

   private void setHeaderPosition(String[] line) {
      for (int idx = 0; idx < line.length; ++idx) {
         Header header = Header.fromString(line[idx]);
         if (header != null) {
            headerPosition.put(header, idx);
         }
      }      
   }

   private boolean isAllHeadersPositionValid() {      
      for (Header header : Header.values()) {         
         if (!headerPosition.containsKey(header)) {
            return false;
         }
      }
      return true;
   }

   private double fixThroughput(String[] line) {
      double writeThroughput = Double.parseDouble(line[headerPosition.get(Header.WRITE_THROUGHPUT)]);
      double readThroughput = Double.parseDouble(line[headerPosition.get(Header.READ_THROUGHPUT)]);
      double expectedWritePercentage = Double.parseDouble(line[headerPosition.get(Header.EXPECTED_WRITE_PERCENTAGE)]);      
      
      return Math.min(writeThroughput / expectedWritePercentage,
                      readThroughput / (1 - expectedWritePercentage));
   }

   private String computeOutputFilePath(String inputFilePath) {
      String[] array = inputFilePath.split("\\.");
      int idx = 0;
      String outputFilePath = array[idx++];

      while (idx < array.length - 1) {
         outputFilePath += ".";
         outputFilePath += array[idx++];
      }

      outputFilePath += "-fix.";
      outputFilePath += array[idx];
      return outputFilePath;
   }
}
