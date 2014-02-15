package org.radargun.config;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.NewElasticMaster;
import org.radargun.config.jaxb.BenchConfig;
import org.radargun.config.jaxb.FixedSizeBenchmark;
import org.radargun.config.jaxb.Property;
import org.radargun.config.jaxb.ScalingBenchmark;
import org.radargun.config.jaxb.Stage;
import org.radargun.stages.AbstractBenchmarkStage;
import org.radargun.stages.stressors.systems.System;
import org.radargun.stages.stressors.systems.workloadGenerators.AbstractWorkloadGenerator;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;
import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Helper class for assembling JAXB configs.
 *
 * @author Mircea.Markus@jboss.com //TODO - add support for System.getEnv //TODO - make sure that if a benchmark has
 *         more nodes than the root an exception is thrown
 */
public class JaxbConfigParser extends ConfigParser {

   private static Log log = LogFactory.getLog(JaxbConfigParser.class);


   public MasterConfig parseConfig(String config) throws Exception {
      JAXBContext jc = JAXBContext.newInstance("org.radargun.config.jaxb");
      Unmarshaller unmarshaller = jc.createUnmarshaller();
      BenchConfig benchConfig = (BenchConfig) unmarshaller.unmarshal(new File(config));
      return getMasterConfig(benchConfig);
   }

   private MasterConfig getMasterConfig(BenchConfig benchConfig) {
      org.radargun.config.jaxb.Master master = benchConfig.getMaster();
      int port = master.getPort() != null ? toInt(master.getPort()) : NewElasticMaster.DEFAULT_PORT;
      MasterConfig masterConfig = new MasterConfig(port, master.getBind(), toInt(master.getSlavesCount()));
      for (ScalingBenchmark sb : benchConfig.getScalingBenchmark()) {
         ScalingBenchmarkConfig sbc = new ScalingBenchmarkConfig();
         sbc.setProductName(sb.getProductName());
         sbc.setConfigName(sb.getConfigName());
         sbc.setInitSize(toInt(sb.getInitSize()));
         sbc.setMaxSize(toInt(sb.getMaxSize()));
         sbc.setIncrement(toInt(sb.getIncrement()));

         List<Stage> benchmarkStagesFromXml = sb.getBenchmarkStages().getStage();
         sbc.setOriginalStages(processStages(benchmarkStagesFromXml));


         sbc.validate();
         masterConfig.addBenchmark(sbc);
      }
      for (FixedSizeBenchmark fb : benchConfig.getFixedSizeBenchmark()) {
         FixedSizeBenchmarkConfig fbc = new FixedSizeBenchmarkConfig();
         fbc.setProductName(fb.getProductName());
         fbc.setConfigName(fb.getConfigName());
         fbc.setSize(toInt(fb.getSize()));
         List<Stage> stagesFromXml = fb.getStage();
         fbc.setOriginalStages(processStages(stagesFromXml));
         fbc.validate();
         masterConfig.addBenchmark(fbc);
      }
      masterConfig.validate();
      return masterConfig;
   }

   private List<org.radargun.Stage> processStages(List<Stage> stagesFromXml) {
      List<org.radargun.Stage> result = new ArrayList<org.radargun.Stage>();
      for (Stage stage : stagesFromXml) {
         List<Property> list = stage.getProperty();
         org.radargun.Stage st = getStage(stage.getName());
         Map<String, String> simpleProps = new HashMap<String, String>();
         Map<String, Map> aggregatedProps = new HashMap<String, Map>();
         for (Property prop : list) {
            if (prop.getMapAggregator() == null) {
               simpleProps.put(prop.getName(), prop.getValue());
            } else {
               Map aggregator = aggregatedProps.get(prop.getMapAggregator());
               if (aggregator == null) {
                  aggregator = new HashMap();
                  aggregatedProps.put(prop.getMapAggregator(), aggregator);
               }
               aggregator.put(prop.getName(), prop.getValue());
            }
         }
         ConfigHelper.setValues(st, simpleProps, true);
         setAggregatedValues(st, aggregatedProps);
         result.add(st);
      }
      return result;
   }

   private void setAggregatedValues(org.radargun.Stage st, Map<String, Map> aggregatedProps) {
      for (String propName : aggregatedProps.keySet()) {
         String setterName = "set" + propName.substring(0, 1).toUpperCase() + propName.substring(1);
         Method method;
         try {
            method = st.getClass().getMethod(setterName, Map.class);
         } catch (NoSuchMethodException e) {
            String errorMsg = "Could not find a setter '" + setterName + "' on class " + st.getClass();
            log.error(errorMsg);
            throw new RuntimeException(e);
         }
         try {
            method.invoke(st, aggregatedProps.get(propName));
         } catch (Throwable e) {
            log.error(e);
            throw new RuntimeException(e);
         }
      }
   }


   public static System getSystem(String systemName) {
      if (systemName.indexOf('.') < 0) {
         systemName = "org.radargun.stages.stressors.systems." + systemName;
      }
      try {
         org.radargun.stages.stressors.systems.System obj;
         Constructor c = Class.forName(systemName).getConstructor();
         obj = (System) c.newInstance();
         return obj;
      } catch (Exception e) {
         String s = "Could not create system of type: " + systemName;
         log.error(s);
         throw new RuntimeException(e);
      }
   }


   public static AbstractWorkloadGenerator getWorkloadGenerator(String workloadGeneratorName, AbstractBenchmarkStage stage) {
      if (workloadGeneratorName.indexOf('.') < 0) {
         workloadGeneratorName = "org.radargun.stages.stressors.systems.workloadGenerators." + workloadGeneratorName;
      }
      try {
         AbstractWorkloadGenerator obj;
         Constructor c = Class.forName(workloadGeneratorName).getConstructor(AbstractBenchmarkStage.class);
         obj = (AbstractWorkloadGenerator) c.newInstance(stage);
         return obj;
      } catch (Exception e) {
         String s = "Could not create stage of type: " + workloadGeneratorName;
         log.error(s);
         throw new RuntimeException(e);
      }
   }

   public static org.radargun.Stage getStage(String stageName) {
      if (stageName.indexOf('.') < 0) {
         stageName = "org.radargun.stages." + stageName;
      }
      try {
         return (org.radargun.Stage) Class.forName(stageName).newInstance();
      } catch (Exception e) {
         String s = "Could not create stage of type: " + stageName;
         log.error(s);
         throw new RuntimeException(e);
      }
   }

   private static int toInt(String str) {
      return Integer.parseInt(str);
   }
}
