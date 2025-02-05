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

import jakarta.jms.ConnectionFactory;
import jakarta.jms.Destination;
import jakarta.jms.JMSConsumer;
import jakarta.jms.JMSContext;

/**
 * Simple JMS message consumer, as program.
 *
 * @author Chris de Vreeze
 */
public class SimpleJmsConsumer {

    private static final String QUEUE_NAME = System.getProperty("queue", "DEV.QUEUE.1");
    private static final long TIMEOUT = Long.parseLong(System.getProperty("timeout", "5000"));

    public static void main(String[] args) {
        ConnectionFactory cf = ConnectionFactories.newConnectionFactory();

        String nextMessage = receiveNextMessage(QUEUE_NAME, cf);
        System.out.println(nextMessage);
    }

    public static String receiveNextMessage(String queueName, ConnectionFactory connectionFactory) {
        try (JMSContext jmsContext = connectionFactory.createContext()) {
            Destination destination = jmsContext.createQueue(queueName);
            JMSConsumer jmsConsumer = jmsContext.createConsumer(destination);
            return jmsConsumer.receiveBody(String.class, TIMEOUT);
        }
    }
}
