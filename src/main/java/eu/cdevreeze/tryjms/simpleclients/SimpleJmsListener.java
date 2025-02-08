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

import com.google.common.collect.ImmutableList;
import jakarta.jms.*;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.LogManager;
import java.util.logging.Logger;

/**
 * Simple JMS message consumer using a MessageListener, as program.
 *
 * @author Chris de Vreeze
 */
public class SimpleJmsListener {

    private static final Logger logger = LogManager.getLogManager().getLogger(Logger.GLOBAL_LOGGER_NAME);

    private static final String QUEUE_NAME = System.getProperty("queue", "DEV.QUEUE.1");
    private static final int MAX_MESSAGES = Integer.parseInt(System.getProperty("maxMessages", "9"));
    private static final long MAX_WAIT_TIME_IN_SEC = Long.parseLong(System.getProperty("maxWaitTimeInSec", "120"));

    public static void main(String[] args) {
        logger.info("Current thread: " + Thread.currentThread());

        ConnectionFactory cf = ConnectionFactories.newConnectionFactory();

        List<String> messages = receiveMessages(QUEUE_NAME, cf);

        logger.info("Current thread: " + Thread.currentThread());
        messages.forEach(System.out::println);
    }

    public static List<String> receiveMessages(String queueName, ConnectionFactory connectionFactory) {
        try (JMSContext jmsContext = connectionFactory.createContext(JMSContext.CLIENT_ACKNOWLEDGE);
             JMSConsumer jmsConsumer = jmsContext.createConsumer(jmsContext.createQueue(queueName))) {

            logger.info("Current thread: " + Thread.currentThread());

            AtomicReference<ImmutableList<String>> messages = new AtomicReference<>(ImmutableList.of());
            CountDownLatch countDownLatch = new CountDownLatch(1);

            jmsConsumer.setMessageListener(new MyMessageListener(messages, countDownLatch));

            countDownLatch.await(MAX_WAIT_TIME_IN_SEC, TimeUnit.SECONDS);

            logger.info("Current thread: " + Thread.currentThread());

            return messages.get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static final class MyMessageListener implements MessageListener {

        private final AtomicReference<ImmutableList<String>> messages;
        private final CountDownLatch countDownLatch;

        public MyMessageListener(AtomicReference<ImmutableList<String>> messages, CountDownLatch countDownLatch) {
            this.messages = messages;
            this.countDownLatch = countDownLatch;
        }

        @Override
        public void onMessage(Message message) {
            logger.info("Receiving message. Current thread: " + Thread.currentThread());

            // This is important, because otherwise a message is auto-acknowledged without being returned in the message collection
            if (countDownLatch.getCount() >= 1) {
                logger.info("Acknowledging message. Current thread: " + Thread.currentThread());

                try {
                    message.acknowledge();

                    ImmutableList<String> updatedMessages = addTextOfTextMessage(message, messages);

                    if (updatedMessages.size() >= MAX_MESSAGES) {
                        countDownLatch.countDown();
                    }
                } catch (JMSException e) {
                    throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
                }
            }
        }

        private static ImmutableList<String> addTextOfTextMessage(Message message, AtomicReference<ImmutableList<String>> messages) {
            if (message instanceof TextMessage textMessage) {
                return messages.updateAndGet(msgs -> {
                    try {
                        return ImmutableList.<String>builder()
                                .addAll(msgs)
                                .add(textMessage.getText())
                                .build();
                    } catch (JMSException e) {
                        throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
                    }
                });
            } else {
                return messages.get();
            }
        }
    }
}
