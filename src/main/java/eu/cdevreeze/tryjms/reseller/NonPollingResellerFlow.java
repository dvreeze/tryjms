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

import eu.cdevreeze.tryjms.simpleclients.ConnectionFactories;
import eu.cdevreeze.yaidom4j.dom.immutabledom.Element;
import eu.cdevreeze.yaidom4j.dom.immutabledom.jaxpinterop.DocumentParser;
import eu.cdevreeze.yaidom4j.dom.immutabledom.jaxpinterop.DocumentParsers;
import eu.cdevreeze.yaidom4j.dom.immutabledom.jaxpinterop.DocumentPrinter;
import eu.cdevreeze.yaidom4j.dom.immutabledom.jaxpinterop.DocumentPrinters;
import jakarta.jms.*;
import org.xml.sax.InputSource;

import java.io.Console;
import java.io.StringReader;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

/**
 * Reseller JMS flow, communicating with the event booking service. It uses MessageListener instances
 * instead of polling.
 * <p>
 * See <a href="https://developer.ibm.com/learningpaths/ibm-mq-badge/mq-coding-challenge/">mq-coding-challenge</a>.
 * <p>
 * Run this program outside the IDE (such as IntelliJ), in order to have a console! For example:
 * <pre>
 * mvn compile exec:java -Dexec.mainClass="eu.cdevreeze.tryjms.reseller.NonPollingResellerFlow"
 * </pre>
 * <p>
 * This program uses several techniques in its implementation. First of all, it uses `CompletableFuture`,
 * so it uses the combination of `Future` and `CompletionStage`, where the latter API helps in composing
 * asynchronous tasks.
 * <p>
 * Secondly, it uses method `CompletableFuture.runAsync`, passing a *single-threaded* `ExecutorService`
 * as second function argument, e.g. for JMS client code, which is required to be single-threaded. Often,
 * these tasks may create and attach a JMS `MessageListener` to a `JMSConsumer`, where the message
 * listener's `onMessage` method runs in yet another thread (under the control of JMS).
 * <p>
 * Thirdly, "internal messaging" between threads can be accomplished using a `BlockingQueue` (with
 * blocking methods `put` being called in one thread and `take` in the other thread).
 * <p>
 * Fourthly, it uses a `CountDownLatch` *to keep threads alive long enough, but no longer*. One thread
 * calls method `countDown` (from 1 to 0), and the blocked threads that called method `await` on that
 * `CountDownLatch` are thus "woken up", causing them to proceed, which typically means finishing the task
 * they were running.
 * <p>
 * Be careful with `Future` (and `CompletableFuture`), though. On creation, they may or may not immediately
 * start to run. It is not like purely *functional effects* like ZIO effects which are purely
 * lazy and functional (i.e. code as an *immutable value*), and which can be combined into a *result effect*
 * without running anything (until some "unsafe run" method is called to run the entire result effect).
 * <p>
 * Note that in this program at least the following threads occur:
 * <ul>
 * <li>The "main" thread that invokes method `main`</li>>
 * <li>Two threads for the `CompletableFuture` instances that trigger listening for JMS messages, one thread for events and one thread for confirmations</li>>
 * <li>The corresponding JMS-controlled threads for the `MessageListener` instances, running the `onMessage` method</li>>
 * <li>One thread for the `CompletableFuture` instance that takes events from the blocking queue (filled by the `onMessage` call consuming events)</li>
 * </ul>
 *
 * @author Chris de Vreeze
 */
public class NonPollingResellerFlow {

    private static final Logger logger = LogManager.getLogManager().getLogger(Logger.GLOBAL_LOGGER_NAME);
    private static final boolean infoLogging = Boolean.parseBoolean(System.getProperty("infoLogging", "true"));
    private static final Level logLevel = infoLogging ? Level.INFO : Level.FINEST;

    private static final DocumentPrinter docPrinter = DocumentPrinters.instance();

    private static final BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<>();

    private static final String NEW_TICKETS_TOPIC = "newTickets";
    private static final String PURCHASE_QUEUE = "purchase";
    private static final String CONFIRMATION_QUEUE = "confirmation";

    private static final int MAX_NUMBER_OF_EVENTS = Integer.parseInt(System.getProperty("maxNumberOfEvents", "6"));
    private static final int MAX_NUMBER_OF_CONFIRMATIONS = MAX_NUMBER_OF_EVENTS;
    private static final long MAX_WAIT_IN_SEC = 5L * 60;

    private static final Event NULL_EVENT = new Event(-1, "", "", "", "", 0);

    private NonPollingResellerFlow() {
    }

    public static void main(String[] args) {
        ConnectionFactory cf = ConnectionFactories.newConnectionFactory();

        logger.log(logLevel, "main - current thread: " + Thread.currentThread());

        // Single-threaded, which is what we want for threads operating in a JMSContext.
        // It guarantees that per submitted task only 1 worker thread will be used.
        ExecutorService executor1 = Executors.newSingleThreadExecutor();
        ExecutorService executor2 = Executors.newSingleThreadExecutor();
        ExecutorService executor3 = Executors.newSingleThreadExecutor();

        CountDownLatch eventsCountDownLatch = new CountDownLatch(1);

        ConcurrentMap<UUID, TicketsRequest> ticketRequestsByCorrelationId = new ConcurrentHashMap<>();

        CompletableFuture<Void> eventListenerFuture = CompletableFuture.runAsync(
                () -> startEventMessageListener(cf, eventsCountDownLatch),
                executor1
        );

        CountDownLatch confirmationsCountDownLatch = new CountDownLatch(1);

        CompletableFuture<Void> confirmationListenerFuture = CompletableFuture.runAsync(
                () -> startConfirmationMessageListener(cf, confirmationsCountDownLatch, ticketRequestsByCorrelationId),
                executor2
        );

        CompletableFuture<Void> processEventsFuture = CompletableFuture.runAsync(
                () -> startHandlingEvents(cf, ticketRequestsByCorrelationId),
                executor3
        );

        // Initiate orderly shutdown, without blocking this thread, and without killing previously submitted tasks
        executor1.shutdown();
        executor2.shutdown();
        executor3.shutdown();

        logger.log(logLevel, "main - current thread: " + Thread.currentThread());

        try {
            CompletableFuture.allOf(eventListenerFuture, confirmationListenerFuture, processEventsFuture)
                    .get(MAX_WAIT_IN_SEC, TimeUnit.SECONDS);

            logger.log(logLevel, "main - current thread: " + Thread.currentThread());

            confirmationsCountDownLatch.await(MAX_WAIT_IN_SEC, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            logger.log(logLevel, "Throwing exception (wrapped in RuntimeException): " + e);
            throw new RuntimeException(e);
        }
    }

    // I should add an exception handler to the connection
    // Also see https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#receiving-messages-asynchronously

    private static void startEventMessageListener(
            ConnectionFactory cf,
            CountDownLatch countDownLatch
    ) {
        try (JMSContext jmsContext = cf.createContext(JMSContext.CLIENT_ACKNOWLEDGE);
             JMSConsumer jmsConsumer = jmsContext.createConsumer(jmsContext.createTopic(NEW_TICKETS_TOPIC))) {

            logger.log(logLevel, "listening for tickets - current thread: " + Thread.currentThread());

            EventMessageListener messageListener = new EventMessageListener(countDownLatch);
            jmsConsumer.setMessageListener(messageListener);

            countDownLatch.await(MAX_WAIT_IN_SEC, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.log(logLevel, "Throwing exception (wrapped in RuntimeException): " + e);
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

            logger.log(logLevel, "listening for confirmations - current thread: " + Thread.currentThread());

            ConfirmationMessageListener messageListener =
                    new ConfirmationMessageListener(countDownLatch, ticketRequestsByCorrelationId);
            jmsConsumer.setMessageListener(messageListener);

            countDownLatch.await(MAX_WAIT_IN_SEC, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.log(logLevel, "Throwing exception (wrapped in RuntimeException): " + e);
            throw new RuntimeException(e);
        }
    }

    private static void startHandlingEvents(
            ConnectionFactory cf,
            ConcurrentMap<UUID, TicketsRequest> ticketRequestsByCorrelationId
    ) {
        logger.log(logLevel, "buying tickets - current thread: " + Thread.currentThread());

        try (JMSContext jmsContext = cf.createContext()) {
            JMSProducer jmsProducer = jmsContext.createProducer();
            Queue purchaseQueue = jmsContext.createQueue(PURCHASE_QUEUE);
            Queue confirmationQueue = jmsContext.createQueue(CONFIRMATION_QUEUE);

            while (true) {
                Event event = eventQueue.take();

                if (event.equals(NULL_EVENT)) {
                    break;
                }

                TicketsRequest ticketsRequest = askForTicketCount(event);

                logger.log(logLevel, "buying tickets (sending message to queue) - current thread: " + Thread.currentThread());

                Message message = jmsContext.createTextMessage(docPrinter.print(ticketsRequest.toXmlElement()));
                message.setJMSReplyTo(confirmationQueue);
                UUID uuid = UUID.randomUUID();
                message.setJMSCorrelationID(uuid.toString());
                message.setJMSExpiration(Instant.now().plus(15, ChronoUnit.MINUTES).toEpochMilli());

                ticketRequestsByCorrelationId.put(uuid, ticketsRequest);

                logger.log(logLevel, "Complete message to send: " + message);

                jmsProducer.send(purchaseQueue, message);
            }
        } catch (InterruptedException e) {
            logger.log(logLevel, "Throwing exception (wrapped in RuntimeException): " + e);
            throw new RuntimeException(e);
        } catch (JMSException e) {
            logger.log(logLevel, "Throwing exception (wrapped in JMSRuntimeException): " + e);
            throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
        }
    }

    private static TicketsRequest askForTicketCount(Event event) {
        Console console = Objects.requireNonNull(System.console());
        console.printf("Event:%n");
        console.printf("%s%n", docPrinter.print(event.toXmlElement()));
        console.printf("There are %d tickets available for %s.%n", event.capacity(), event.title());
        console.printf("How many tickets do you want to secure for this event? ");
        console.flush();

        // Blocking wait for user input. Do not wait too long to answer, or else the server may be down!

        int defaultTicketCount = 10;
        int numberOfTickets;
        try {
            numberOfTickets = Integer.parseInt(console.readLine());
        } catch (RuntimeException e) {
            logger.log(logLevel, "Ignoring exception while reading from console: " + e);
            numberOfTickets = defaultTicketCount;
        }
        return new TicketsRequest(
                event.eventID(),
                event.title(),
                event.date(),
                event.time(),
                event.location(),
                numberOfTickets
        );
    }

    public static final class EventMessageListener implements MessageListener {

        private final DocumentParser docParser =
                DocumentParsers.builder().removingInterElementWhitespace().build();

        private final CountDownLatch countDownLatch;
        private final AtomicInteger eventCounter = new AtomicInteger(0);

        public EventMessageListener(CountDownLatch countDownLatch) {
            this.countDownLatch = countDownLatch;
        }

        @Override
        public void onMessage(Message message) {
            try {
                logger.log(logLevel, "EventMessageListener.onMessage - current thread: " + Thread.currentThread());

                if (countDownLatch.getCount() >= 1) {
                    message.acknowledge();

                    if (message instanceof TextMessage textMessage) {
                        int nextEventCounter = eventCounter.incrementAndGet();
                        String xmlMessage = textMessage.getText();
                        logger.log(logLevel, "Received message: " + xmlMessage);
                        Element element = docParser.parse(new InputSource(new StringReader(xmlMessage)))
                                .documentElement();
                        Event event = Event.fromXmlElement(element);

                        eventQueue.put(event);

                        if (nextEventCounter >= MAX_NUMBER_OF_EVENTS) {
                            eventQueue.put(NULL_EVENT);
                            countDownLatch.countDown();
                        }
                    }
                }
            } catch (InterruptedException e) {
                logger.log(logLevel, "Throwing exception (wrapped in RuntimeException): " + e);
                throw new RuntimeException(e);
            } catch (JMSException e) {
                logger.log(logLevel, "Throwing exception (wrapped in JMSRuntimeException): " + e);
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
                logger.log(logLevel, "ConfirmationMessageListener.onMessage - current thread: " + Thread.currentThread());

                if (countDownLatch.getCount() >= 1) {
                    message.acknowledge();

                    if (message instanceof TextMessage textMessage) {
                        int nextEventCounter = confirmationCounter.incrementAndGet();
                        String msg = textMessage.getText();
                        logger.log(logLevel, "Received message: " + msg);
                        logger.log(logLevel, "Complete message: " + textMessage);
                        Confirmation confirmation = Confirmation.fromString(msg);

                        UUID uuid = UUID.fromString(textMessage.getJMSCorrelationID());
                        TicketsRequest ticketsRequest =
                                ticketRequestsByCorrelationId.get(uuid);

                        if (ticketsRequest != null) {
                            System.out.println();
                            logger.log(logLevel, "showing confirmations - current thread: " + Thread.currentThread());

                            TicketsRequestConfirmation ticketsRequestConfirmation =
                                    new TicketsRequestConfirmation(ticketsRequest, confirmation);

                            System.out.printf(
                                    "%s (event ID %d; event title: %s)%n",
                                    ticketsRequestConfirmation,
                                    ticketsRequestConfirmation.ticketsRequest().eventID(),
                                    ticketsRequestConfirmation.ticketsRequest().title());

                            if (ticketsRequestConfirmation.confirmation() == Confirmation.Accepted) {
                                System.out.printf(
                                        "%d tickets secured for event '%s' (event ID %d)!%n",
                                        ticketsRequestConfirmation.ticketsRequest().numberRequested(),
                                        ticketsRequestConfirmation.ticketsRequest().title(),
                                        ticketsRequestConfirmation.ticketsRequest().eventID()
                                );
                            }
                        }

                        if (nextEventCounter >= MAX_NUMBER_OF_CONFIRMATIONS) {
                            countDownLatch.countDown();
                        }
                    }
                }
            } catch (JMSException e) {
                logger.log(logLevel, "Throwing exception (wrapped in JMSRuntimeException): " + e);
                throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
            }
        }
    }
}
