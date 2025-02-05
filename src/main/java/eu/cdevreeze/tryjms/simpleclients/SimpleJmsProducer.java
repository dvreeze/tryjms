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

import jakarta.jms.*;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Simple JMS message producer, as program.
 *
 * @author Chris de Vreeze
 */
public class SimpleJmsProducer {

    private static final String QUEUE_NAME = System.getProperty("queue", "DEV.QUEUE.1");

    public static void main(String[] args) {
        Objects.checkIndex(0, args.length);
        List<String> messages = Arrays.stream(args).toList();

        ConnectionFactory cf = ConnectionFactories.newConnectionFactory();

        sendMessages(messages, QUEUE_NAME, cf);
    }

    public static void sendMessages(List<String> messageTexts, String queueName, ConnectionFactory connectionFactory) {
        try (JMSContext jmsContext = connectionFactory.createContext()) {
            JMSProducer jmsProducer = jmsContext.createProducer();
            Destination destination = jmsContext.createQueue(queueName);

            for (var messageText : messageTexts) {
                TextMessage message = jmsContext.createTextMessage(messageText);
                jmsProducer.send(destination, message);
            }
        }
    }
}
