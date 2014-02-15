package org.radargun;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.IOException;

/**
 * @author Fabio Perfetti
 * @since 4.0
 */
public class NumNodesJmxRequest {

   private static Log log = LogFactory.getLog(NumNodesJmxRequest.class);

   private final String hostname;

   private static final String COMPONENT_PREFIX = "org.radargun:stage=";
   private static final String DEFAULT_COMPONENT = "TpccBenchmark";
   public static final int DEFAULT_JMX_PORT = 9998;

   private final ObjectName benchmarkComponent;
   private final MBeanServerConnection mBeanServerConnection;

   public NumNodesJmxRequest(String hostname) throws IOException, MalformedObjectNameException {
      this(DEFAULT_COMPONENT, hostname, DEFAULT_JMX_PORT);
   }

   public NumNodesJmxRequest(String hostname, int port) throws IOException, MalformedObjectNameException {
      this(DEFAULT_COMPONENT, hostname, port);
   }

   public NumNodesJmxRequest(String component, String hostname, int port) throws IOException, MalformedObjectNameException {
      this.hostname = hostname;
      String connectionUrl = "service:jmx:rmi:///jndi/rmi://" + hostname + ":" + port + "/jmxrmi";

      JMXConnector connector = null;
      try {
         connector = JMXConnectorFactory.connect(new JMXServiceURL(connectionUrl));
         mBeanServerConnection = connector.getMBeanServerConnection();
         benchmarkComponent = new ObjectName(COMPONENT_PREFIX + component);
      } catch (IOException e) {
         throw e;
      } catch (MalformedObjectNameException e) {
         throw e;
      }
   }

   public void doRequest() {
      if (benchmarkComponent == null) {
         throw new NullPointerException("Component does not exists");
      }

      try {
         mBeanServerConnection.invoke(benchmarkComponent, "changeNumNodes", new Object[0], new String[0]);
         log.info("Slave " + hostname + " notified");
      } catch (InstanceNotFoundException e) {
         throw new RuntimeException(e);
      } catch (MBeanException e) {
         throw new RuntimeException(e);
      } catch (ReflectionException e) {
         throw new RuntimeException(e);
      } catch (IOException e) {
         throw new RuntimeException(e);
      }
   }
}
