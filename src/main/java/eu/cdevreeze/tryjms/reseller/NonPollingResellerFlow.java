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
import com.google.common.collect.ImmutableMap;
import eu.cdevreeze.tryjms.simpleclients.ConnectionFactories;
import eu.cdevreeze.yaidom4j.core.NamespaceScope;
import eu.cdevreeze.yaidom4j.dom.immutabledom.Element;
import eu.cdevreeze.yaidom4j.dom.immutabledom.NodeBuilder.ConciseApi;
import eu.cdevreeze.yaidom4j.dom.immutabledom.jaxpinterop.DocumentParser;
import eu.cdevreeze.yaidom4j.dom.immutabledom.jaxpinterop.DocumentParsers;
import eu.cdevreeze.yaidom4j.dom.immutabledom.jaxpinterop.DocumentPrinter;
import eu.cdevreeze.yaidom4j.dom.immutabledom.jaxpinterop.DocumentPrinters;
import jakarta.jms.*;
import org.xml.sax.InputSource;

import java.io.Console;
import java.io.StringReader;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.LogManager;
import java.util.logging.Logger;

import static eu.cdevreeze.yaidom4j.dom.immutabledom.ElementPredicates.hasName;

/**
 * Reseller JMS flow, communicating with the event booking service. It uses MessageListener instances
 * instead of polling.
 * <p>
 * See <a href="https://developer.ibm.com/learningpaths/ibm-mq-badge/mq-coding-challenge/">mq-coding-challenge</a>.
 *
 * @author Chris de Vreeze
 */
public class NonPollingResellerFlow {

    // The program design is such that the MessageListener instances are running short tasks in
    // a single-threaded way (and therefore also not creating any Futures).

    public record Event(
            int eventID,
            String title,
            String date,
            String time,
            String location,
            int capacity
    ) {

        public Element toXmlElement() {
            ConciseApi api = new ConciseApi(NamespaceScope.empty());

            return api.element(
                    "Event",
                    ImmutableMap.of(),
                    ImmutableList.of(
                            api.textElement("eventID", String.valueOf(eventID)),
                            api.textElement("title", title),
                            api.textElement("date", date),
                            api.textElement("time", time),
                            api.textElement("location", location),
                            api.textElement("capacity", String.valueOf(capacity))
                    )
            );
        }

        public static Event fromXmlElement(Element element) {
            return new Event(
                    element.childElementStream(hasName("eventID"))
                            .map(Element::text)
                            .mapToInt(Integer::parseInt)
                            .findFirst()
                            .orElseThrow(),
                    element.childElementStream(hasName("title"))
                            .map(Element::text)
                            .findFirst()
                            .orElseThrow(),
                    element.childElementStream(hasName("date"))
                            .map(Element::text)
                            .findFirst()
                            .orElseThrow(),
                    element.childElementStream(hasName("time"))
                            .map(Element::text)
                            .findFirst()
                            .orElseThrow(),
                    element.childElementStream(hasName("location"))
                            .map(Element::text)
                            .findFirst()
                            .orElseThrow(),
                    element.childElementStream(hasName("capacity"))
                            .map(Element::text)
                            .mapToInt(Integer::parseInt)
                            .findFirst()
                            .orElseThrow()
            );
        }
    }

    public record TicketsRequest(
            int eventID,
            String title,
            String time,
            String location,
            int numberRequested
    ) {

        public Element toXmlElement() {
            ConciseApi api = new ConciseApi(NamespaceScope.empty());

            return api.element(
                    "RequestTickets",
                    ImmutableMap.of(),
                    ImmutableList.of(
                            api.textElement("eventID", String.valueOf(eventID)),
                            api.textElement("title", title),
                            api.textElement("time", time),
                            api.textElement("location", location),
                            api.textElement("numberRequested", String.valueOf(numberRequested))
                    )
            );
        }
    }

    private static final Logger logger = LogManager.getLogManager().getLogger(Logger.GLOBAL_LOGGER_NAME);

    private static final DocumentPrinter docPrinter = DocumentPrinters.instance();

    private static final String NEW_TICKETS_TOPIC = "newTickets";
    private static final String PURCHASE_QUEUE = "purchase";

    private static final int MAX_NUMBER_OF_EVENTS = Integer.parseInt(System.getProperty("maxNumberOfEvents", "6"));
    private static final long MAX_WAIT_IN_SEC = 5L * 60;

    public static void main(String[] args) {
        ConnectionFactory cf = ConnectionFactories.newConnectionFactory();

        CountDownLatch eventsCountDownLatch = new CountDownLatch(MAX_NUMBER_OF_EVENTS);

        startEventMessageListener(cf, eventsCountDownLatch);

        // Set MessageListener for purchase queue

        try {
            eventsCountDownLatch.await(MAX_WAIT_IN_SEC, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

    }

    private static void startEventMessageListener(ConnectionFactory cf, CountDownLatch countDownLatch) {
        try (JMSContext jmsContext = cf.createContext(JMSContext.CLIENT_ACKNOWLEDGE);
             JMSConsumer jmsConsumer = jmsContext.createConsumer(jmsContext.createTopic(NEW_TICKETS_TOPIC))) {

            logger.info("listenForTickets - current thread: " + Thread.currentThread());

            EventMessageListener messageListener = new EventMessageListener(cf, countDownLatch);
            jmsConsumer.setMessageListener(messageListener);

            countDownLatch.await(MAX_WAIT_IN_SEC, TimeUnit.SECONDS);
        } catch (JMSRuntimeException e) {
            logger.info("Thrown (ignored) JMS unchecked exception: " + e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static void handleEvent(Event event, ConnectionFactory cf) throws InterruptedException {
        TicketsRequest eventTicketCount = askForTicketCount(event);
        buyTickets(eventTicketCount, cf);
    }

    private static TicketsRequest askForTicketCount(Event event) {
        Console console = Objects.requireNonNull(System.console());
        console.printf("Event:%n");
        console.printf(docPrinter.print(event.toXmlElement()) + "%n");
        console.printf("How many tickets do you want to buy for this event? ");
        // Blocking wait for user input. Do not wait too long to answer, or else data gets lost!
        int numberOfTickets = Integer.parseInt(console.readLine());
        return new TicketsRequest(
                event.eventID,
                event.title,
                event.time,
                event.location,
                numberOfTickets
        );
    }

    private static void buyTickets(TicketsRequest ticketsRequest, ConnectionFactory cf) {
        logger.info("buyTickets - current thread: " + Thread.currentThread());

        try (JMSContext jmsContext = cf.createContext()) {
            JMSProducer jmsProducer = jmsContext.createProducer();
            Queue purchaseQueue = jmsContext.createQueue(PURCHASE_QUEUE);

            logger.info("buyTickets (sending message to queue) - current thread: " + Thread.currentThread());

            jmsProducer.send(purchaseQueue, docPrinter.print(ticketsRequest.toXmlElement()));
        }
    }

    public static final class EventMessageListener implements MessageListener {

        private final DocumentParser docParser =
                DocumentParsers.builder().removingInterElementWhitespace().build();

        private final ConnectionFactory cf;
        private final CountDownLatch countDownLatch;
        private final AtomicInteger eventCounter = new AtomicInteger(0);

        public EventMessageListener(ConnectionFactory cf, CountDownLatch countDownLatch) {
            this.cf = cf;
            this.countDownLatch = countDownLatch;
        }

        @Override
        public void onMessage(Message message) {
            try {
                logger.info("EventMessageListener.onMessage - current thread: " + Thread.currentThread());

                if (countDownLatch.getCount() >= 1) {
                    message.acknowledge();

                    if (message instanceof TextMessage textMessage) {
                        eventCounter.incrementAndGet();
                        String xmlMessage = textMessage.getText();
                        logger.info("Received message: " + xmlMessage);
                        Element element = docParser.parse(new InputSource(new StringReader(xmlMessage)))
                                .documentElement();

                        handleEvent(Event.fromXmlElement(element), cf);

                        if (eventCounter.get() >= MAX_NUMBER_OF_EVENTS) {
                            countDownLatch.countDown();
                        }
                    }
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (JMSException e) {
                throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
            }
        }
    }
}
