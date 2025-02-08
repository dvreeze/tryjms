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

import java.io.StringReader;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Scanner;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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
            // Maybe make these similar queries easier in yaidom4j?
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
            String date,
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
                            api.textElement("date", date),
                            api.textElement("time", time),
                            api.textElement("location", location),
                            api.textElement("numberRequested", String.valueOf(numberRequested))
                    )
            );
        }
    }

    public enum Confirmation {
        Accepted, Rejected;

        public static Confirmation fromString(String s) {
            return switch (s) {
                case "Accepted" -> Accepted;
                case "Rejected" -> Rejected;
                default -> throw new RuntimeException("Not a confirmation: " + s);
            };
        }
    }

    private static final Logger logger = LogManager.getLogManager().getLogger(Logger.GLOBAL_LOGGER_NAME);

    private static final DocumentPrinter docPrinter = DocumentPrinters.instance();

    private static final String NEW_TICKETS_TOPIC = "newTickets";
    private static final String PURCHASE_QUEUE = "purchase";
    private static final String CONFIRMATION_QUEUE = "confirmation";

    private static final int MAX_NUMBER_OF_EVENTS = Integer.parseInt(System.getProperty("maxNumberOfEvents", "6"));
    private static final int MAX_NUMBER_OF_CONFIRMATIONS = MAX_NUMBER_OF_EVENTS;
    private static final long MAX_WAIT_IN_SEC = 5L * 60;

    public static void main(String[] args) {
        ConnectionFactory cf = ConnectionFactories.newConnectionFactory();

        CountDownLatch eventsCountDownLatch = new CountDownLatch(1);

        ConcurrentMap<UUID, TicketsRequest> ticketRequestsByCorrelationId = new ConcurrentHashMap<>();

        startEventMessageListener(cf, eventsCountDownLatch, ticketRequestsByCorrelationId);

        CountDownLatch confirmationsCountDownLatch = new CountDownLatch(1);

        startConfirmationMessageListener(cf, confirmationsCountDownLatch, ticketRequestsByCorrelationId);

        try {
            confirmationsCountDownLatch.await(MAX_WAIT_IN_SEC, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.info("Throwing exception (wrapped in RuntimeException): " + e);
            throw new RuntimeException(e);
        }
    }

    // I should add an exception handler to the connection

    private static void startEventMessageListener(
            ConnectionFactory cf,
            CountDownLatch countDownLatch,
            ConcurrentMap<UUID, TicketsRequest> ticketRequestsByCorrelationId
    ) {
        try (JMSContext jmsContext = cf.createContext(JMSContext.CLIENT_ACKNOWLEDGE);
             JMSConsumer jmsConsumer = jmsContext.createConsumer(jmsContext.createTopic(NEW_TICKETS_TOPIC))) {

            logger.info("listenForTickets - current thread: " + Thread.currentThread());

            EventMessageListener messageListener =
                    new EventMessageListener(jmsContext, countDownLatch, ticketRequestsByCorrelationId);
            jmsConsumer.setMessageListener(messageListener);

            countDownLatch.await(MAX_WAIT_IN_SEC, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.info("Throwing exception (wrapped in RuntimeException): " + e);
            throw new RuntimeException(e);
        }
    }

    private static void startConfirmationMessageListener(
            ConnectionFactory cf,
            CountDownLatch countDownLatch,
            ConcurrentMap<UUID, TicketsRequest> ticketRequestsByCorrelationId
    ) {
        try (JMSContext jmsContext = cf.createContext(JMSContext.CLIENT_ACKNOWLEDGE);
             JMSConsumer jmsConsumer = jmsContext.createConsumer(jmsContext.createQueue(CONFIRMATION_QUEUE))) {

            logger.info("listenForConfirmations - current thread: " + Thread.currentThread());

            ConfirmationMessageListener messageListener =
                    new ConfirmationMessageListener(countDownLatch, ticketRequestsByCorrelationId);
            jmsConsumer.setMessageListener(messageListener);

            countDownLatch.await(MAX_WAIT_IN_SEC, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.info("Throwing exception (wrapped in RuntimeException): " + e);
            throw new RuntimeException(e);
        }
    }

    private static void handleEvent(
            Event event,
            JMSContext jmsContext,
            ConcurrentMap<UUID, TicketsRequest> ticketRequestsByCorrelationId
    ) throws InterruptedException {
        TicketsRequest eventTicketCount = askForTicketCount(event);
        buyTickets(eventTicketCount, jmsContext, ticketRequestsByCorrelationId);
    }

    private static TicketsRequest askForTicketCount(Event event) {
        // Often System.console() returns null, so we have to work around that
        System.out.printf("Event:%n");
        System.out.printf("%s%n", docPrinter.print(event.toXmlElement()));
        System.out.printf("There are %d tickets available for %s.%n", event.capacity, event.title);
        System.out.print("How many tickets do you want to secure for this event? ");
        System.out.flush();
        // Blocking wait for user input. Do not wait too long to answer, or else the server may be down!
        try (Scanner scanner = new Scanner(System.in)) {
            int defaultTicketCount = 10;
            int numberOfTickets = scanner.hasNextInt() ? scanner.nextInt() : defaultTicketCount;
            return new TicketsRequest(
                    event.eventID,
                    event.title,
                    event.date,
                    event.time,
                    event.location,
                    numberOfTickets
            );
        }
    }

    private static void buyTickets(
            TicketsRequest ticketsRequest,
            JMSContext jmsContext,
            ConcurrentMap<UUID, TicketsRequest> ticketRequestsByCorrelationId
    ) {
        logger.info("buyTickets - current thread: " + Thread.currentThread());

        try {
            JMSProducer jmsProducer = jmsContext.createProducer();
            Queue purchaseQueue = jmsContext.createQueue(PURCHASE_QUEUE);
            Queue confirmationQueue = jmsContext.createQueue(CONFIRMATION_QUEUE);

            logger.info("buyTickets (sending message to queue) - current thread: " + Thread.currentThread());

            Message message = jmsContext.createTextMessage(docPrinter.print(ticketsRequest.toXmlElement()));
            message.setJMSReplyTo(confirmationQueue);
            UUID uuid = UUID.randomUUID();
            message.setJMSCorrelationID(uuid.toString());
            message.setJMSExpiration(Instant.now().plus(15, ChronoUnit.MINUTES).toEpochMilli());

            ticketRequestsByCorrelationId.put(uuid, ticketsRequest);

            logger.info("Complete message to send: " + message);

            jmsProducer.send(purchaseQueue, message);
        } catch (JMSException e) {
            logger.info("Throwing exception (wrapped in JMSRuntimeException): " + e);
            throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
        }
    }

    public static final class EventMessageListener implements MessageListener {

        private final DocumentParser docParser =
                DocumentParsers.builder().removingInterElementWhitespace().build();

        private final JMSContext jmsContext;
        private final CountDownLatch countDownLatch;
        private final ConcurrentMap<UUID, TicketsRequest> ticketRequestsByCorrelationId;
        private final AtomicInteger eventCounter = new AtomicInteger(0);

        public EventMessageListener(
                JMSContext jmsContext,
                CountDownLatch countDownLatch,
                ConcurrentMap<UUID, TicketsRequest> ticketRequestsByCorrelationId) {
            this.jmsContext = jmsContext;
            this.countDownLatch = countDownLatch;
            this.ticketRequestsByCorrelationId = ticketRequestsByCorrelationId;
        }

        @Override
        public void onMessage(Message message) {
            try {
                logger.info("EventMessageListener.onMessage - current thread: " + Thread.currentThread());

                if (countDownLatch.getCount() >= 1) {
                    message.acknowledge();

                    if (message instanceof TextMessage textMessage) {
                        int nextEventCounter = eventCounter.incrementAndGet();
                        String xmlMessage = textMessage.getText();
                        logger.info("Received message: " + xmlMessage);
                        Element element = docParser.parse(new InputSource(new StringReader(xmlMessage)))
                                .documentElement();

                        handleEvent(Event.fromXmlElement(element), jmsContext, ticketRequestsByCorrelationId);

                        if (nextEventCounter >= MAX_NUMBER_OF_EVENTS) {
                            countDownLatch.countDown();
                        }
                    }
                }
            } catch (InterruptedException e) {
                logger.info("Throwing exception (wrapped in RuntimeException): " + e);
                throw new RuntimeException(e);
            } catch (JMSException e) {
                logger.info("Throwing exception (wrapped in JMSRuntimeException): " + e);
                throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
            }
        }
    }

    public static final class ConfirmationMessageListener implements MessageListener {

        private final CountDownLatch countDownLatch;
        private final ConcurrentMap<UUID, TicketsRequest> ticketRequestsByCorrelationId;
        private final AtomicInteger confirmationCounter = new AtomicInteger(0);

        public ConfirmationMessageListener(
                CountDownLatch countDownLatch,
                ConcurrentMap<UUID, TicketsRequest> ticketRequestsByCorrelationId
        ) {
            this.countDownLatch = countDownLatch;
            this.ticketRequestsByCorrelationId = ticketRequestsByCorrelationId;
        }

        @Override
        public void onMessage(Message message) {
            try {
                logger.info("ConfirmationMessageListener.onMessage - current thread: " + Thread.currentThread());

                if (countDownLatch.getCount() >= 1) {
                    message.acknowledge();

                    if (message instanceof TextMessage textMessage) {
                        int nextEventCounter = confirmationCounter.incrementAndGet();
                        String msg = textMessage.getText();
                        logger.info("Received message: " + msg);
                        logger.info("Complete message: " + textMessage);
                        Confirmation confirmation = Confirmation.fromString(msg);

                        UUID uuid = UUID.fromString(textMessage.getJMSCorrelationID());
                        TicketsRequest ticketsRequest =
                                ticketRequestsByCorrelationId.get(uuid);

                        if (ticketsRequest == null) {
                            System.out.println("Could not find ticket request for correlation ID " + uuid);
                        } else {
                            System.out.printf(
                                    "%s (event ID %d; event title: %s)%n",
                                    confirmation,
                                    ticketsRequest.eventID(),
                                    ticketsRequest.title());

                            if (confirmation == Confirmation.Accepted) {
                                System.out.printf(
                                        "%d tickets secured for event '%s' (event ID %d)!%n",
                                        ticketsRequest.numberRequested(),
                                        ticketsRequest.title(),
                                        ticketsRequest.eventID()
                                );
                            }
                        }

                        if (nextEventCounter >= MAX_NUMBER_OF_CONFIRMATIONS) {
                            countDownLatch.countDown();
                        }
                    }
                }
            } catch (JMSException e) {
                logger.info("Throwing exception (wrapped in JMSRuntimeException): " + e);
                throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
            }
        }
    }
}
