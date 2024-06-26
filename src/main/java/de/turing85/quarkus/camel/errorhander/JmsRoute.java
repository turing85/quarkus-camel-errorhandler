package de.turing85.quarkus.camel.errorhander;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.jms.ConnectionFactory;

import io.agroal.api.AgroalDataSource;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.camel.Exchange;
import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.JtaTransactionErrorHandlerBuilder;
import org.apache.camel.builder.RouteBuilder;
import org.eclipse.microprofile.context.ManagedExecutor;

import static org.apache.camel.builder.endpoint.StaticEndpointBuilders.*;

@Slf4j
@ApplicationScoped
@RequiredArgsConstructor
public class JmsRoute extends RouteBuilder {
  public static final String NUMBER_RECEIVER_TO_DB = "number-receiver-to-db";
  public static final String DB_WRITER = "db-writer";
  public static final String TOPIC = "numbers";
  public static final String QUEUE = "numbers-to-db";
  public static final String GLOBAL_STOP_VARIABLE = "global:stop";

  private final ConnectionFactory connectionFactory;
  private final AgroalDataSource dataSource;
  private final ManagedExecutor executor;

  @Override
  public void configure() {
    // @formatter:off
    errorHandler(new JtaTransactionErrorHandlerBuilder()
        .asyncDelayedRedelivery()
        .maximumRedeliveries(3)
        .onExceptionOccurred(exchange -> log.info(
            "Exception caught {}",
            exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Exception.class).toString()))
        .onRedelivery(exchange -> {
          final int redeliveryCounter =
              exchange.getIn().getHeader(Exchange.REDELIVERY_COUNTER, int.class);
          final int maxRedeliveries =
              exchange.getIn().getHeader(Exchange.REDELIVERY_MAX_COUNTER, int.class);
          log.info(
              "Redelivery {} / {}",
              redeliveryCounter,
              maxRedeliveries);
          if (redeliveryCounter >= maxRedeliveries) {
            log.info("Stopping all routes");
            exchange.getContext().setVariable(GLOBAL_STOP_VARIABLE, Boolean.TRUE);
            exchange.setRollbackOnly(true);
          }
        }));

    interceptFrom("*")
        .when(variable(GLOBAL_STOP_VARIABLE).isEqualTo(true))
            .log("stopping (received on ${exchange.getFromRouteId()})")
            .setVariable(GLOBAL_STOP_VARIABLE).constant(false)
            .process(exchange -> executor.submit(
                () -> JmsRoute.suspendAllRoutes(exchange)))
            .markRollbackOnly()
            .stop()
        .end();

    from(
        jms("topic:%s".formatted(TOPIC))
            .cacheLevelName("CACHE_CONSUMER")
            .concurrentConsumers(5)
            .connectionFactory(connectionFactory)
            .subscriptionShared(true)
            .subscriptionDurable(true)
            .durableSubscriptionName(QUEUE)
            .transacted(true))
        .routeId(NUMBER_RECEIVER_TO_DB)
        .transacted()
        .log(LoggingLevel.DEBUG, "Receiving ${body}")
        .to(direct(DB_WRITER));

    from(direct(DB_WRITER))
        .routeId(DB_WRITER)
        .transacted("PROPAGATION_REQUIRES_NEW")
        .convertBodyTo(int.class)
        .log(LoggingLevel.DEBUG, "Receiving ${body}")
        .to(sql("INSERT INTO numbers(value) VALUES(:#${body})")
            .dataSource(dataSource));
    // @formatter:on
  }

  private static Void suspendAllRoutes(Exchange exchange) throws Exception {
    exchange.getContext().getRouteController().stopAllRoutes();
    return null;
  }
}
