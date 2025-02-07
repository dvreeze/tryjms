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

package eu.cdevreeze.tryjms.reseller;

import com.google.common.collect.ImmutableList;
import eu.cdevreeze.tryjms.simpleclients.ConnectionFactories;
import eu.cdevreeze.yaidom4j.dom.immutabledom.Element;
import eu.cdevreeze.yaidom4j.dom.immutabledom.jaxpinterop.DocumentParser;
import eu.cdevreeze.yaidom4j.dom.immutabledom.jaxpinterop.DocumentParsers;
import eu.cdevreeze.yaidom4j.dom.immutabledom.jaxpinterop.DocumentPrinter;
import eu.cdevreeze.yaidom4j.dom.immutabledom.jaxpinterop.DocumentPrinters;
import jakarta.jms.*;
import org.xml.sax.InputSource;

import java.io.StringReader;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.LogManager;
import java.util.logging.Logger;

/**
 * Reseller JMS flow, communicating with the event booking service. It uses a MessageListener
 * instead of polling.
 * <p>
 * See <a href="https://developer.ibm.com/learningpaths/ibm-mq-badge/mq-coding-challenge/">mq-coding-challenge</a>.
 *
 * @author Chris de Vreeze
 */
public class NonPollingResellerFlow {

    private static final Logger logger = LogManager.getLogManager().getLogger(Logger.GLOBAL_LOGGER_NAME);

    private static final String NEW_TICKETS_TOPIC = "newTickets";

    private static final long TIMEOUT = 4L * 60 * 1000;

    public static void main(String[] args) {
        ConnectionFactory cf = ConnectionFactories.newConnectionFactory();

        List<Element> eventElements = retrieveEvents(cf);

        DocumentPrinter docPrinter = DocumentPrinters.instance();

        List<String> eventsAsStrings = eventElements.stream().map(docPrinter::print).toList();

        eventsAsStrings.forEach(System.out::println);
    }

    private static List<Element> retrieveEvents(ConnectionFactory cf) {
        AtomicReference<ImmutableList<Element>> elements =
                new AtomicReference<>(ImmutableList.of());

        try (JMSContext jmsContext = cf.createContext();
             JMSConsumer jmsConsumer = jmsContext.createConsumer(jmsContext.createTopic(NEW_TICKETS_TOPIC))) {

            logger.info("main - current thread: " + Thread.currentThread());

            EventMessageListener messageListener = new EventMessageListener(elements);
            jmsConsumer.setMessageListener(messageListener);

            Thread.sleep(TIMEOUT); // Improve!

            logger.info("main - current thread: " + Thread.currentThread());

            return elements.get();
        } catch (InterruptedException e) {
            return elements.get();
        } catch (JMSRuntimeException e) {
            logger.info("Thrown (ignored) exception: " + e);
            return elements.get();
        }
    }

    public static final class EventMessageListener implements MessageListener {

        private final DocumentParser docParser =
                DocumentParsers.builder().removingInterElementWhitespace().build();

        private final AtomicReference<ImmutableList<Element>> elements;

        public EventMessageListener(AtomicReference<ImmutableList<Element>> elements) {
            this.elements = elements;
        }

        @Override
        public void onMessage(Message message) {
            try {
                logger.info("onMessage - current thread: " + Thread.currentThread());

                if (message instanceof TextMessage textMessage) {
                    String xmlMessage = textMessage.getText();
                    logger.info("Received message: " + xmlMessage);
                    Element element = docParser.parse(new InputSource(new StringReader(xmlMessage)))
                            .documentElement();

                    elements.updateAndGet(elems ->
                            ImmutableList.<Element>builder()
                                    .addAll(elems)
                                    .add(element)
                                    .build()
                    );
                }
            } catch (JMSException e) {
                throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
            }
        }
    }
}
