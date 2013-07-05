package org.radargun;

import javax.management.*;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.IOException;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.Map;

/**
 *
 * @author Fabio Perfetti
 * @since 4.0
 */
public class NumNodesJmxRequest {

    private static final String COMPONENT_PREFIX = "org.radargun:stage=";
    private static final String DEFAULT_COMPONENT = "TpccBenchmark";
    public static final int DEFAULT_JMX_PORT = 9998;

    private final ObjectName benchmarkComponent;
    private final MBeanServerConnection mBeanServerConnection;

    public NumNodesJmxRequest(String hostname) {
        this(DEFAULT_COMPONENT, hostname, DEFAULT_JMX_PORT);
    }

    public NumNodesJmxRequest(String hostname, int port) {
        this(DEFAULT_COMPONENT, hostname, port);
    }

    public NumNodesJmxRequest(String component, String hostname, int port) {
        String connectionUrl = "service:jmx:rmi:///jndi/rmi://" + hostname + ":" + port + "/jmxrmi";

        JMXConnector connector = null;
        try {
            connector = JMXConnectorFactory.connect(new JMXServiceURL(connectionUrl));
            mBeanServerConnection = connector.getMBeanServerConnection();
            benchmarkComponent = new ObjectName(COMPONENT_PREFIX + component);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (MalformedObjectNameException e) {
            throw new RuntimeException(e);
        }

    }

    public void doRequest() {
        if (benchmarkComponent == null) {
            throw new NullPointerException("Component does not exists");
        }

        try {
            mBeanServerConnection.invoke(benchmarkComponent, "changeNumNodes", new Object[0], new String[0]);
            System.out.println("New nodes notified!");
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
