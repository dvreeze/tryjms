/*
 * Copyright 2024-2024 Chris de Vreeze
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package eu.cdevreeze.tryjms.simpleclients;

import jakarta.jms.ConnectionFactory;

import java.lang.reflect.InvocationTargetException;

/**
 * ConnectionFactory factory.
 *
 * @author Chris de Vreeze
 */
public class ConnectionFactories {

    private static final String WMQ_HOST_NAME = "XMSC_WMQ_HOST_NAME";
    private static final String WMQ_PORT = "XMSC_WMQ_PORT";
    private static final String WMQ_CHANNEL = "XMSC_WMQ_CHANNEL";
    private static final String WMQ_CONNECTION_MODE = "XMSC_WMQ_CONNECTION_MODE";
    private static final String WMQ_QUEUE_MANAGER = "XMSC_WMQ_QUEUE_MANAGER";
    private static final String USER_AUTHENTICATION_MQCSP = "XMSC_USER_AUTHENTICATION_MQCSP";
    private static final String USERID = "XMSC_USERID";
    private static final String PASSWORD = "XMSC_PASSWORD";
    private static final int WMQ_CM_CLIENT = 1;

    public static ConnectionFactory newConnectionFactory() {
        try {
            Class<?> wmqConstantsClass =
                    Class.forName("com.ibm.msg.client.jakarta.wmq.WMQConstants");
            Class<?> jmsFactoryFactoryClass =
                    Class.forName("com.ibm.msg.client.jakarta.jms.JmsFactoryFactory");

            // Must be "com.ibm.msg.client.jakarta.wmq"
            String jakartaWmqProviderName = (String) wmqConstantsClass.getField("JAKARTA_WMQ_PROVIDER").get(null);
            Object jmsFactoryFactory = jmsFactoryFactoryClass
                    .getMethod("getInstance", String.class)
                    .invoke(null, jakartaWmqProviderName);

            ConnectionFactory cf = (ConnectionFactory) jmsFactoryFactoryClass
                    .getMethod("createConnectionFactory")
                    .invoke(jmsFactoryFactory);

            setStringProperty(cf, WMQ_HOST_NAME, System.getProperty("hostName", "localhost"));
            setIntProperty(cf, WMQ_PORT, Integer.parseInt(System.getProperty("port", "1414")));
            setStringProperty(cf, WMQ_CHANNEL, System.getProperty("channel", "DEV.APP.SVRCONN"));
            setIntProperty(cf, WMQ_CONNECTION_MODE, WMQ_CM_CLIENT);
            setStringProperty(cf, WMQ_QUEUE_MANAGER, System.getProperty("queueManager", "QM1"));
            setBooleanProperty(cf, USER_AUTHENTICATION_MQCSP, true);
            setStringProperty(cf, USERID, "app");
            setStringProperty(cf, PASSWORD, "passw0rd");
            return cf;
        } catch (ClassNotFoundException | NoSuchMethodException | InvocationTargetException | IllegalAccessException |
                 NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }

    private static void setStringProperty(ConnectionFactory cf, String constantName, String constantValue) {
        try {
            cf.getClass().getMethod("setStringProperty", String.class, String.class)
                    .invoke(cf, constantName, constantValue);
        } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    private static void setIntProperty(ConnectionFactory cf, String constantName, int constantValue) {
        try {
            cf.getClass().getMethod("setIntProperty", String.class, int.class)
                    .invoke(cf, constantName, constantValue);
        } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    private static void setBooleanProperty(ConnectionFactory cf, String constantName, boolean constantValue) {
        try {
            cf.getClass().getMethod("setBooleanProperty", String.class, boolean.class)
                    .invoke(cf, constantName, constantValue);
        } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }
}
