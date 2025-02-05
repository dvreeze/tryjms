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

package eu.cdevreeze.tryjms.helloworld;

import com.ibm.msg.client.jakarta.jms.JmsConnectionFactory;
import com.ibm.msg.client.jakarta.jms.JmsFactoryFactory;
import com.ibm.msg.client.jakarta.wmq.WMQConstants;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSException;
import jakarta.jms.JMSRuntimeException;

/**
 * ConnectionFactory factory.
 *
 * @author Chris de Vreeze
 */
public class ConnectionFactories {

    public static ConnectionFactory newConnectionFactory() {
        try {
            JmsFactoryFactory ff = JmsFactoryFactory.getInstance("com.ibm.msg.client.jakarta.wmq");
            JmsConnectionFactory cf = ff.createConnectionFactory();
            cf.setStringProperty(WMQConstants.WMQ_HOST_NAME, System.getProperty("hostName", "localhost"));
            cf.setIntProperty(WMQConstants.WMQ_PORT, Integer.parseInt(System.getProperty("port", "1414")));
            cf.setStringProperty(WMQConstants.WMQ_CHANNEL, System.getProperty("channel", "DEV.APP.SVRCONN"));
            cf.setIntProperty(WMQConstants.WMQ_CONNECTION_MODE, WMQConstants.WMQ_CM_CLIENT);
            cf.setStringProperty(WMQConstants.WMQ_QUEUE_MANAGER, System.getProperty("queueManager", "QM1"));
            cf.setBooleanProperty(WMQConstants.USER_AUTHENTICATION_MQCSP, true);
            cf.setStringProperty(WMQConstants.USERID, "app");
            cf.setStringProperty(WMQConstants.PASSWORD, "passw0rd");
            return cf;
        } catch (JMSException e) {
            throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
        }
    }
}
