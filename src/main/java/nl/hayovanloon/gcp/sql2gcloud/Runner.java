package nl.hayovanloon.gcp.sql2gcloud;

import com.google.common.collect.ImmutableList;
import io.atlassian.fugue.Option;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import rx.Observable;
import rx.Subscriber;
import rx.Subscription;
import rx.observables.ConnectableObservable;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.security.GeneralSecurityException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;


public class Runner {

  private static final Logger LOG = LogManager.getLogger(Runner.class);

  private final Config config;

  public static void main(String[] args) throws IOException {
    final Config config = Config.parseArgs(args);
    final List<String> configErrors = config.validate();
    if (!configErrors.isEmpty()) {
      System.err.println("Configuration errors detected:");
      configErrors.forEach(error -> System.err.println("  " + error));
      System.err.println("Operation aborted.");
      System.err.println("\nRun without command line arguments to display help.");
      System.exit(1);
    }

    Runner r = Runner.with(config);
    r.run(config);
  }

  private Runner(Config config) {
    this.config = config;
  }

  public static Runner with(Config config) {
    return new Runner(config);
  }

  public void run(Config config) {
    // Register JDBC driver if provided
    config.getDriver().forEach(driver -> {
      try {
        Class.forName(driver).newInstance();
      } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
        System.err.println("exception while registering JDBC driver: " + e);
        System.exit(1);
      }
    });

    if (config.getDriver().isEmpty()) {
      final List<String> jvmArgs = ManagementFactory.getRuntimeMXBean().getInputArguments();
      if (!jvmArgs.stream().anyMatch(x -> x.startsWith("-Djdbc.drivers"))) {
        LOG.warn("No JDBC driver specified and could not detect one in JVM parameters.");
      }
    }

    Option<Subscription> subscription = Option.none();
    try(Connection connection = DriverManager.getConnection(config.getDatabase(), config.getUser(),
        config.getPassword())) {
      final Observable<List<String>> observable = getObservable(connection, config.getQuery());
      final Subscriber<List<String>> subscriber = GFSSubscriber.of(config.getBucket(),
          config.getFile(), config.getSeparator());

      subscription = Option.some(observable.subscribe(subscriber));

    } catch (SQLException | GeneralSecurityException | IOException e) {
      System.err.println("Exception during initiation, operation aborted: " + e);
      e.printStackTrace();
      subscription.forEach(Subscription::unsubscribe);
      System.exit(1);
    }
  }

  private static Observable<List<String>> getObservable(Connection connection, String query)
      throws GeneralSecurityException, IOException {

    return ConnectableObservable.create(
        new Observable.OnSubscribe<List<String>>() {

          @Override
          public void call(Subscriber<? super List<String>> subscriber) {
            try {
              final PreparedStatement statement = connection.prepareStatement(query);
              LOG.info("Started reading");
              final ResultSet resultSet = statement.executeQuery();
              final int cols = resultSet.getMetaData().getColumnCount();
              while (resultSet.next()) {
                ImmutableList.Builder<String> colData = ImmutableList.builder();
                for (int i = 1; i <= cols; i += 1) colData.add(resultSet.getString(i));
                subscriber.onNext(colData.build());
              }
              LOG.info("Done reading");
              subscriber.onCompleted();
              connection.close();
            } catch (SQLException e) {
              subscriber.onError(e);
              try {
                connection.close();
              } catch (SQLException e1) {
                LOG.warn("Exception while closing connection: {}", e1);
              }
            }
          }
        }
    );
  }
}
