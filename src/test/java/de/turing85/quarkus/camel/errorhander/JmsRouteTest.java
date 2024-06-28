package de.turing85.quarkus.camel.errorhander;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.stream.IntStream;

import jakarta.inject.Inject;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSConsumer;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSException;
import jakarta.jms.JMSProducer;
import jakarta.jms.Message;
import jakarta.jms.Queue;
import jakarta.ws.rs.core.Response;

import com.google.common.truth.Truth;
import io.agroal.api.AgroalDataSource;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.RestAssured;
import lombok.extern.slf4j.Slf4j;
import org.apache.camel.CamelContext;
import org.apache.camel.Route;
import org.apache.camel.ServiceStatus;
import org.apache.camel.builder.AdviceWith;
import org.apache.camel.spi.RouteController;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static java.util.function.Predicate.not;

@QuarkusTest
@Slf4j
class JmsRouteTest {
  public static final String THROWER_ID = "thrower";

  @Inject
  CamelContext camelContext;

  @Inject
  @SuppressWarnings("CdiInjectionPointsInspection")
  ConnectionFactory connectionFactory;

  @Inject
  @SuppressWarnings("CdiInjectionPointsInspection")
  AgroalDataSource dataSource;

  private final Random random = new Random();

  @BeforeEach
  void setup() throws Exception {
    stopAllRoutes();
    emptyQueue();
    emptyTable();
    startAllRoutes();
  }

  @Test
  void sendMessage() throws SQLException {
    // given
    final int numberToSend = random.nextInt(1_000_000);

    // when
    sendToTopic(numberToSend);

    // then
    assertHealthUp();
    assertDbHasNEntriesForValue(1, numberToSend);
    assertNoMoreMessagesOnQueue();
  }

  @Test
  void failOnNumberReceiver() throws Exception {
    try {
      // given
      addThrowerToRoute(JmsRoute.NUMBER_RECEIVER_TO_DB);
      final int numberToSend = random.nextInt(1_000_000);

      // when
      sendToTopic(numberToSend);

      // then
      assertHealthDown();
      assertDbHasNEntriesForValue(3, numberToSend);
      assertMessageOnQueue(numberToSend);
    } finally {
      // cleanup
      removeThrowerFromRoute(JmsRoute.NUMBER_RECEIVER_TO_DB);
    }
  }

  @Test
  void failOnDbWriter() throws Exception {
    try {
      // given
      addThrowerToRoute(JmsRoute.DB_WRITER);
      final int numberToSend = random.nextInt(1_000_000);

      // when
      sendToTopic(numberToSend);

      // then
      assertHealthDown();
      assertDbHasNEntriesForValue(0, numberToSend);
      assertMessageOnQueue(numberToSend);
    } finally {
      // cleanup
      removeThrowerFromRoute(JmsRoute.DB_WRITER);
    }
  }

  @Test
  void loadTest() throws SQLException {
    // given
    final int numberOfValues = 10_000;
    final List<Integer> values = IntStream.range(0, numberOfValues)
        .map(unused -> random.nextInt(100_000_000)).boxed().toList();

    // when
    sendToTopic(values);

    // then
    final long started = System.nanoTime();
    assertDbHasNEntries(numberOfValues, Duration.ofSeconds(100));
    final Duration consumed = Duration.ofNanos(System.nanoTime() - started);
    assertDbHasEntries(values);
    assertNoMoreMessagesOnQueue();
    // @formatter:off
    log.info(
        "{} messages took {}.{} seconds to process",
        numberOfValues,
        consumed.toSeconds(),
        consumed.getNano());
    // @formatter:on
    Truth.assertThat(consumed).isLessThan(Duration.ofSeconds(10));
  }

  private void stopAllRoutes() throws Exception {
    camelContext.getRouteController().stopAllRoutes();
    assertAllRoutesHaveStatus(ServiceStatus.Stopped);
    assertHealthDown();
  }

  private void startAllRoutes() {
    camelContext.setVariable(JmsRoute.GLOBAL_STOP_VARIABLE, false);
    // @formatter:off
    final RouteController routeController = camelContext.getRouteController();
    camelContext.getRoutes().stream()
        .map(Route::getRouteId)
        .toList().stream()
        .filter(not(routeId -> routeController.getRouteStatus(routeId).isStarted()))
        .forEach(routeId -> {
          try {
            routeController.startRoute(routeId);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });
    // @formatter:on
    assertAllRoutesHaveStatus(ServiceStatus.Started);
    assertHealthUp();
  }

  private void assertAllRoutesHaveStatus(ServiceStatus status) {
    // @formatter:off
    camelContext.getRoutes().stream()
        .map(Route::getRouteId)
        .forEach(routeId -> Awaitility.await()
            .atMost(Duration.ofSeconds(1))
            .untilAsserted(() -> Truth
                .assertThat(camelContext.getRouteController().getRouteStatus(routeId))
                .isEqualTo(status)));
    // @formatter:on
  }

  private void emptyQueue() throws JMSException {
    try (final JMSContext context = connectionFactory.createContext()) {
      final Queue queueDestination =
          context.createQueue("%s::%s".formatted(JmsRoute.TOPIC, JmsRoute.QUEUE));
      final JMSConsumer consumer = context.createConsumer(queueDestination);
      while (true) {
        final Message message = consumer.receive(Duration.ofSeconds(1).toMillis());
        if (Objects.nonNull(message)) {
          message.acknowledge();
        } else {
          break;
        }
      }
    }
  }

  private void emptyTable() throws SQLException {
    try (final Statement statement = dataSource.getConnection().createStatement()) {
      statement.execute("TRUNCATE TABLE numbers");
    }
  }

  private void addThrowerToRoute(String routeId) throws Exception {
    // @formatter:off
    AdviceWith.adviceWith(
        camelContext,
        routeId,
        advice -> advice.weaveAddLast()
            .throwException(new Exception("Exception to test transaction")).id(THROWER_ID));
    // @formatter:on
  }

  private void removeThrowerFromRoute(String routeId) throws Exception {
    // @formatter:off
    AdviceWith.adviceWith(
        camelContext,
        routeId,
        advice -> advice.weaveById(THROWER_ID).remove());
    // @formatter:on
  }

  private void sendToTopic(int bodyToSend) {
    try (final JMSContext context = connectionFactory.createContext()) {
      context.createProducer().send(context.createTopic(JmsRoute.TOPIC), bodyToSend);
    }
  }

  private void sendToTopic(List<Integer> bodiesToSend) {
    try (final JMSContext context = connectionFactory.createContext();
        final JMSContext transactedContext = context.createContext(JMSContext.SESSION_TRANSACTED)) {
      final JMSProducer transactedProducer = transactedContext.createProducer();
      bodiesToSend.forEach(
          bodyToSend -> transactedProducer.send(context.createTopic(JmsRoute.TOPIC), bodyToSend));
      transactedContext.commit();
    }
  }

  private static void assertHealthUp() {
    // @formatter:off
    Awaitility.await()
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(() -> RestAssured
            .when().get("/q/health")
            .then().statusCode(Response.Status.OK.getStatusCode()));
    // @formatter:on
  }

  private static void assertHealthDown() {
    // @formatter:off
    Awaitility.await()
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(() -> RestAssured
            .when().get("/q/health")
            .then().statusCode(Response.Status.SERVICE_UNAVAILABLE.getStatusCode()));
    // @formatter:on
  }

  private void assertDbHasNEntries(int n, Duration duration) throws SQLException {
    try (final Statement statement = dataSource.getConnection().createStatement()) {
      // @formatter:off
      final int[] currentDots = { 0 };
      Awaitility.await()
          .atMost(duration)
          .untilAsserted(() -> {
            final ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM numbers");
            Truth.assertThat(rs.next()).isTrue();
            final int count = rs.getInt(1);
            final int dots = (int) (count * (10d / n));
            if (dots > currentDots[0]) {
              currentDots[0] = dots;
              log.info(".".repeat(currentDots[0]));
            }
            Truth.assertThat(count).isEqualTo(n);
          });
      // @formatter:on
    }
  }

  private void assertDbHasEntries(List<Integer> values) throws SQLException {
    // @formatter:off
    try (final Statement statement =
             dataSource.getConnection()
                 .createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE)) {
      Awaitility.await()
          .atMost(Duration.ofSeconds(10))
          .untilAsserted(() -> {
            final ResultSet rs = statement.executeQuery("SELECT value FROM numbers");
            rs.last();
            Truth.assertThat(rs.getRow()).isEqualTo(values.size());
            rs.first();
            final List<Integer> actual = new ArrayList<>();
            do {
              actual.add(rs.getInt(1));
            } while (rs.next());
            Truth.assertThat(actual).containsExactlyElementsIn(values);
          });
    }
    // @formatter:on
  }

  private void assertDbHasNEntriesForValue(int n, int value) throws SQLException {
    try (final Statement statement = dataSource.getConnection().createStatement()) {
      // @formatter:off
      Awaitility.await()
          .atMost(Duration.ofSeconds(10))
          .untilAsserted(() -> {
            final ResultSet rs = statement
                .executeQuery("SELECT COUNT(*) FROM numbers WHERE value = %s".formatted(value));
            Truth.assertThat(rs.next()).isTrue();
            Truth.assertThat(rs.getInt(1)).isEqualTo(n);
          });
      // @formatter:on
    }
  }

  private void assertNoMoreMessagesOnQueue() {
    try (final JMSContext context = connectionFactory.createContext()) {
      final Queue queueDestination =
          context.createQueue("%s::%s".formatted(JmsRoute.TOPIC, JmsRoute.QUEUE));
      final JMSConsumer consumer = context.createConsumer(queueDestination);
      final Message message = consumer.receive(Duration.ofSeconds(1).toMillis());
      Truth.assertThat(message).isNull();
    }
  }

  private void assertMessageOnQueue(int expectedBody) throws JMSException {
    try (final JMSContext context = connectionFactory.createContext()) {
      final Queue queueDestination =
          context.createQueue("%s::%s".formatted(JmsRoute.TOPIC, JmsRoute.QUEUE));
      final JMSConsumer consumer = context.createConsumer(queueDestination);
      final Message message = consumer.receive(Duration.ofSeconds(5).toMillis());
      Truth.assertThat(message).isNotNull();
      Truth.assertThat(message.getBody(Integer.class)).isEqualTo(expectedBody);
      message.acknowledge();
    }
  }
}
