package org.radargun.config;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.Master;
import org.radargun.Stage;
import org.radargun.stages.AbstractBenchmarkStage;
import org.radargun.stages.GenerateScalingChartStage;
import org.radargun.stages.StartClusterStage;
import org.radargun.stages.stressors.systems.OpenSystem;
import org.radargun.stages.stressors.systems.SystemType;
import org.radargun.stages.stressors.systems.workloadGenerators.AbstractWorkloadGenerator;
import org.radargun.utils.TypedProperties;
import org.w3c.dom.*;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @author Mircea.Markus@jboss.com
 */
public class DomConfigParser extends ConfigParser {

    private static Log log = LogFactory.getLog(DomConfigParser.class);

    public MasterConfig parseConfig(String config) throws ParserConfigurationException {
        //the content in the new file is too dynamic, let's just use DOM for now

        DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();

        Document document;
        try {
            document = builder.parse(config);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }

        Element configRoot = (Element) document.getElementsByTagName("bench-config").item(0);

        ScalingBenchmarkConfig prototype = buildBenchmarkPrototype(configRoot);

        MasterConfig masterConfig = parseMaster(configRoot, prototype);

        parseProductsElement(configRoot, prototype, masterConfig);

        //now add the reporting
        parseReporting(configRoot, masterConfig);

        return masterConfig;

    }


    private void parseReporting(Element configRoot, MasterConfig masterConfig) {
        Element reportsEl = (Element) configRoot.getElementsByTagName("reports").item(0);
        NodeList reportElList = reportsEl.getElementsByTagName("report");
        FixedSizeBenchmarkConfig reportBenchmark = new FixedSizeBenchmarkConfig();

        masterConfig.addBenchmark(reportBenchmark);
        for (int i = 0; i < reportElList.getLength(); i++) {
            if (reportElList.item(i) instanceof Element) {
                Element thisReportEl = (Element) reportElList.item(i);

                GenerateScalingChartStage generateChartStage = new GenerateScalingChartStage();
                reportBenchmark.addOriginalStage(generateChartStage);

//                GenerateChartStage generateChartStage = new GenerateChartStage();
//                reportBenchmark.addStage(generateChartStage);

                generateChartStage.setFnPrefix(ConfigHelper.getStrAttribute(thisReportEl, "name"));
                if (thisReportEl.getAttribute("includeAll") != null) {
                    String inclAll = ConfigHelper.getStrAttribute(thisReportEl, "includeAll");
                    if (inclAll.equalsIgnoreCase("true"))
                        continue;
                }

                NodeList itemsEl = thisReportEl.getElementsByTagName("item");
                for (int j = 0; j < itemsEl.getLength(); j++) {
                    Element itemEl = (Element) itemsEl.item(j);
                    String productName = ConfigHelper.getStrAttribute(itemEl, "product");
                    String productConfig = ConfigHelper.getStrAttribute(itemEl, "config");
                    generateChartStage.addReportFilter(productName, productConfig);
                }
            }
        }
    }

    private void parseProductsElement(Element configRoot, ScalingBenchmarkConfig prototype, MasterConfig masterConfig) {
        Element productsEl = (Element) configRoot.getElementsByTagName("products").item(0);
        NodeList productsChildEl = productsEl.getChildNodes();
        for (int i = 0; i < productsChildEl.getLength(); i++) {
            Node node = productsChildEl.item(i);
            if (node instanceof Element) {
                Element nodeEl = (Element) node;
                String productName = nodeEl.getNodeName();
                NodeList configs = nodeEl.getElementsByTagName("config");
                for (int configIndex = 0; configIndex < configs.getLength(); configIndex++) {
                    Element configEl = (Element) configs.item(configIndex);
                    String configName = configEl.getAttribute("name");
                    Properties configAttributes = getAttributes(configEl);

                    ScalingBenchmarkConfig clone = prototype.clone();
                    clone.setProductName(productName);
                    masterConfig.addBenchmark(clone);
                    clone.setConfigName(configName);
                    clone.setConfigAttributes(new TypedProperties(configAttributes));
                    updateStartupStage(configName, clone);
                }

            }
        }
    }

    public static Properties getAttributes(Element configEl) {
        NamedNodeMap attributes = configEl.getAttributes();
        Properties configAttributes = new Properties();
        for (int j = 0; j < attributes.getLength(); j++) {
            String name = attributes.item(j).getNodeName();
            String value = attributes.item(j).getNodeValue();
            configAttributes.put(name, value);
        }
        return configAttributes;
    }

    private MasterConfig parseMaster(Element configRoot, ScalingBenchmarkConfig prototype) {
        MasterConfig masterConfig;
        Element masterEl = (Element) configRoot.getElementsByTagName("master").item(0);
        String bindAddress = ConfigHelper.getStrAttribute(masterEl, "bindAddress");
        int port = masterEl.getAttribute("port") != null ? ConfigHelper.getIntAttribute(masterEl, "port") : Master.DEFAULT_PORT;
        masterConfig = new MasterConfig(port, bindAddress, prototype.getMaxSize());
        return masterConfig;
    }

    private void updateStartupStage(String configName, ScalingBenchmarkConfig clone) {
        for (Stage st : clone.getOriginalStages()) {

            if (st instanceof StartClusterStage) {
                StartClusterStage scs = (StartClusterStage) st;
                scs.setConfig(configName);
                scs.setConfAttributes(clone.getConfigAttributes());
            }
        }
    }

    private ScalingBenchmarkConfig buildBenchmarkPrototype(Element configRoot) {
        ScalingBenchmarkConfig prototype;
        prototype = new ScalingBenchmarkConfig();
        Element benchmarkEl = (Element) configRoot.getElementsByTagName("benchmark").item(0);
        prototype.setInitSize(ConfigHelper.getIntAttribute(benchmarkEl, "initSize"));
        prototype.setMaxSize(ConfigHelper.getIntAttribute(benchmarkEl, "maxSize"));
        prototype.setIncrement(ConfigHelper.getIntAttribute(benchmarkEl, "increment"));

        NodeList childNodes = benchmarkEl.getChildNodes();
        for (int i = 0; i < childNodes.getLength(); i++) {
            Node child = childNodes.item(i);
            if (child instanceof Element) {
                Element childEl = (Element) child;
                String stageShortName = childEl.getNodeName();
                Stage st = JaxbConfigParser.getStage(stageShortName + "Stage");
                prototype.addOriginalStage(st);
                NamedNodeMap attributes = childEl.getAttributes();
                Map<String, String> attrToSet = new HashMap<String, String>();
                for (int attrIndex = 0; attrIndex < attributes.getLength(); attrIndex++) {
                    Attr attr = (Attr) attributes.item(attrIndex);
                    attrToSet.put(attr.getName(), ConfigHelper.parseString(attr.getValue()));
                }
                ConfigHelper.setValues(st, attrToSet, true);

                // Setting system
                if (st instanceof AbstractBenchmarkStage) {
                    //let's read the system
                    NodeList stageChildNodes = childEl.getChildNodes();
                    for (int j = 0; j < stageChildNodes.getLength(); j++) {
                        Node systemChild = stageChildNodes.item(j);
                        if (systemChild instanceof Element) {
                            Element systemEl = (Element) systemChild;
                            String systemName = systemEl.getNodeName();
                            SystemType system = JaxbConfigParser.getSystem(systemName);
                            if (system == null)
                                throw new RuntimeException("Unvalid System type");
                            ((AbstractBenchmarkStage) st).setSysyemType(system);
                            NamedNodeMap systemAttributes = systemEl.getAttributes();
                            Map<String, String> systemAttrToSet = new HashMap<String, String>();
                            for (int attrIndex = 0; attrIndex < systemAttributes.getLength(); attrIndex++) {
                                Attr attr = (Attr) systemAttributes.item(attrIndex);
                                systemAttrToSet.put(attr.getName(), ConfigHelper.parseString(attr.getValue()));
                            }
                            ConfigHelper.setValues(system, systemAttrToSet, true);

                            if (system.getType().equals(SystemType.OPEN)) { // or istanceof OpenSystem
                                //let's read the workloadGenerator
                                NodeList systemChildNodes = systemEl.getChildNodes();
                                for (int k = 0; k < systemChildNodes.getLength(); k++) {
                                    Node workloadChild = systemChildNodes.item(k);
                                    if (workloadChild instanceof Element) {
                                        Element generatorEl = (Element) workloadChild;
                                        String generatorShortName = generatorEl.getNodeName();
                                        AbstractWorkloadGenerator wg = JaxbConfigParser.getWorkloadGenerator(generatorShortName + "WorkloadGenerator", (AbstractBenchmarkStage) st);
                                        if (wg == null)
                                            throw new RuntimeException("Unvalid Workload Generator");
                                        ((OpenSystem) system).setWorkloadGenerator(wg);
                                        NamedNodeMap loadGenAttributes = generatorEl.getAttributes();
                                        Map<String, String> loadGenAttrToSet = new HashMap<String, String>();
                                        for (int attrIndex = 0; attrIndex < loadGenAttributes.getLength(); attrIndex++) {
                                            Attr attr = (Attr) loadGenAttributes.item(attrIndex);
                                            loadGenAttrToSet.put(attr.getName(), ConfigHelper.parseString(attr.getValue()));
                                        }
                                        ConfigHelper.setValues(wg, loadGenAttrToSet, true);
                                    }
                                }
                            }
                        }
                    }
                }

//                // Setting workload generator
//                if (st instanceof AbstractBenchmarkStage) {
//                    //let's read the workloadGenerator
//                    NodeList stageChildNodes = childEl.getChildNodes();
//                    for (int j = 0; j < stageChildNodes.getLength(); j++) {
//                        Node workloadChild = stageChildNodes.item(j);
//                        if (workloadChild instanceof Element) {
//                            Element generatorEl = (Element) workloadChild;
//                            String generatorShortName = generatorEl.getNodeName();
//                            AbstractWorkloadGenerator wg = JaxbConfigParser.getWorkloadGenerator(generatorShortName + "WorkloadGenerator", (AbstractBenchmarkStage) st);
//                            if (wg == null)
//                                throw new RuntimeException("Unvalid Workload Generator");
//                            ((AbstractBenchmarkStage) st).setWorkloadGenerator(wg);
//                            NamedNodeMap loadGenAttributes = generatorEl.getAttributes();
//                            Map<String, String> loadGenAttrToSet = new HashMap<String, String>();
//                            for (int attrIndex = 0; attrIndex < loadGenAttributes.getLength(); attrIndex++) {
//                                Attr attr = (Attr) loadGenAttributes.item(attrIndex);
//                                loadGenAttrToSet.put(attr.getName(), ConfigHelper.parseString(attr.getValue()));
//                            }
//                            ConfigHelper.setValues(wg, loadGenAttrToSet, true);
//                        }
//                    }
//                    if (((AbstractBenchmarkStage) st).getWorkloadGenerator() == null) {
//                        AbstractWorkloadGenerator wg = new ClosedWorkloadGenerator( ((AbstractBenchmarkStage) st) );
//                        ((AbstractBenchmarkStage) st).setWorkloadGenerator(wg);
//                    }
//                }

            }
        }
        return prototype;
    }



//    @Deprecated
//    private DynamicBenchmarkConfig buildDynamicBenchmarkPrototype(Element configRoot) {
//        DynamicBenchmarkConfig prototype;
//        prototype = new DynamicBenchmarkConfig();
//        Element benchmarkEl = (Element) configRoot.getElementsByTagName("benchmark").item(0);
//
//        prototype.setMaxSize(ConfigHelper.getIntAttribute(benchmarkEl, "maxSize"));
//        /* prototype.setTxLowerBound(ConfigHelper.getIntAttribute(benchmarkEl,"txLowerBound"));
//       prototype.setTxUpperBound(ConfigHelper.getIntAttribute(benchmarkEl,"txUpperBound")); */
//
//        NodeList childNodes = benchmarkEl.getChildNodes();
//        for (int i = 0; i < childNodes.getLength(); i++) {
//            Node child = childNodes.item(i);
//            if (child instanceof Element) {
//                Element childEl = (Element) child;
//                String stageShortName = childEl.getNodeName();
//                Stage st = JaxbConfigParser.getStage(stageShortName + "Stage");
//                prototype.addStage(st);
//                NamedNodeMap attributes = childEl.getAttributes();
//                Map<String, String> attrToSet = new HashMap<String, String>();
//                for (int attrIndex = 0; attrIndex < attributes.getLength(); attrIndex++) {
//                    Attr attr = (Attr) attributes.item(attrIndex);
//                    attrToSet.put(attr.getName(), ConfigHelper.parseString(attr.getValue()));
//                }
//                ConfigHelper.setValues(st, attrToSet, true);
//            }
//        }
//
//        /** Vado a preparare lo stack da eseguire negli slavi "ritardari"**/
//        benchmarkEl = (Element) configRoot.getElementsByTagName("benchmark-scaling").item(0);
//
//        childNodes = benchmarkEl.getChildNodes();
//        for (int i = 0; i < childNodes.getLength(); i++) {
//            Node child = childNodes.item(i);
//            if (child instanceof Element) {
//                Element childEl = (Element) child;
//                String stageShortName = childEl.getNodeName();
//                Stage st = JaxbConfigParser.getStage(stageShortName + "Stage");
//                prototype.addScalingStage(st);
//                NamedNodeMap attributes = childEl.getAttributes();
//                Map<String, String> attrToSet = new HashMap<String, String>();
//                for (int attrIndex = 0; attrIndex < attributes.getLength(); attrIndex++) {
//                    Attr attr = (Attr) attributes.item(attrIndex);
//                    attrToSet.put(attr.getName(), ConfigHelper.parseString(attr.getValue()));
//                }
//                ConfigHelper.setValues(st, attrToSet, true);
//            }
//        }
//
//        return prototype;
//    }


}
