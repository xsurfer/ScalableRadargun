package org.radargun;


import org.radargun.config.ConfigParser;
import org.radargun.config.MasterConfig;

import java.io.File;

/**
 * @author Mircea.Markus@jboss.com
 */
public class LaunchMaster {

   public static void main(String[] args) {

      File currentDir = new File(".");
      String message = "Running in directory: " + currentDir.getAbsolutePath();
      out(message);

      String config = getConfigOrExit(args);

      out("Configuration file is: " + config);

      ConfigParser configParser = ConfigParser.getConfigParser();
      //configParser è DomConfigParser
      MasterConfig masterConfig = null;
      try {
         masterConfig = configParser.parseConfig(config);
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
      NewElasticMaster server = new NewElasticMaster(masterConfig);
      server.start();
   }

   private static String getConfigOrExit(String[] args) {
      String config = null;
      for (int i = 0; i < args.length - 1; i++) {
         if (args[i].equals("-config")) {
            config = args[i + 1];
         }
      }
      if (config == null) {
         printUsageAndExit();
      }
      return config;
   }

   private static void printUsageAndExit() {
      System.out.println("Usage: master.sh  -config <config-file.xml>");
      System.out.println("       -config : xml file containing benchmark's configuration");
      ShutDownHook.exit(1);
   }


   private static void launchOld(String config) throws Exception {
      File configFile = new File(config);
      if (!configFile.exists()) {
         System.err.println("No such file: " + configFile.getAbsolutePath());
         printUsageAndExit();
      }
   }

   private static void out(String message) {
      System.out.println(message);
   }
}
