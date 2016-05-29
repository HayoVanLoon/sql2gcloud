package nl.hayovanloon.gcp.sql2gcloud;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import io.atlassian.fugue.Option;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import rx.Observable;
import rx.Subscriber;
import rx.Subscription;
import rx.observables.ConnectableObservable;

import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.atlassian.fugue.Option.option;


public class Runner {

  private static final Logger LOG = LogManager.getLogger(Runner.class);
  private static final Pattern PARAMS_PATTERN = Pattern.compile(
      "(" +
          "(\\s+-c\\s+(?<conf>[^\\s]+))" +
          "|(\\s+-d\\s+(?<database>jdbc:[^\\s]+))" +
          "|(\\s+-u\\s+(?<user>[^\\s]+))" +
          "|(\\s+-p\\s+'(?<password>[^']+)')" +
          "|(\\s+-s\\s+'(?<separator>[^']+)'))+" +
          "(\\s+gs://(?<bucket>[a-z0-9-_.]{3,63})/(?<file>[^\\s]+))?" +
          "(\\s+(?<query>.+))?");
  private static final String HELP =
      "Usage:" +
          "\n  sql2gcloud [-c <config>] [-d <database>] [-u <user>] [-p '<password>'] [-s '<separator>'] [<dest_url>] [<query>]" +
          "\n" +
          "\nExample:" +
          "\n  sql2gcloud -c conf/config.json gs://foo/bar/bla.txt SELECT * FROM foo" +
          "\n" +
          "\nConfig file format (JSON):" +
          "\n{" +
          "\n  \"database\": \"jdbc:mysql://127.0.0.1:3306/my_database\"," +
          "\n  \"user\": \"my_user\"," +
          "\n  \"password\": \"my_password\"," +
          "\n  \"bucket\": \"foo\"," +
          "\n  \"file\": \"bar/bla.txt\"," +
          "\n  \"separator\": \"~~\"," +
          "\n  \"query\": \"SELECT * FROM foo;\"" +
          "\n}" +
          "\n" +
          "\nSeparator is optional (default is '~~', all other fields must be " +
          "\nspecified in the config file or passed as a command line argument." +
          "\n" +
          "\nWhen passed via the command line, bucket and file names are to be" +
          "\nmerged into a single 'gs://...' url.";
  private static final String MISSING_PARAM = "either include it in the config or pass it as a parameter";

  public static void main(String[] args) throws IOException {
    final Config config = parseArgs(args);
    final List<String> configErrors = config.validate();
    if (!configErrors.isEmpty()) {
      System.err.println("Configuration errors detected:");
      configErrors.forEach(error -> System.err.println("  " + error));
      System.err.println("Operation aborted.");
      System.err.println("\nRun without command line arguments to display help.");
      System.exit(1);
    }

    // Register MySQL driver
    try {
      Class.forName("com.mysql.jdbc.Driver").newInstance();
    } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
      System.err.println("exception while registering MySQL driver: " + e);
      System.exit(1);
    }

    Option<Subscription> subscription = Option.none();
    try(Connection connection = DriverManager.getConnection(config.getDatabase(), config.getUser(),
        config.getPassword())) {
      final Observable<List<String>> observable = getObservable(connection, config.getQuery());
      final Subscriber<List<String>> subscriber = GFSSubscriber.of(connection, config.getBucket(),
          config.getFile(), config.getSeparator());

      subscription = Option.some(observable.subscribe(subscriber));

    } catch (SQLException | GeneralSecurityException | IOException e) {
      System.err.println("Exception during initiation, operation aborted: " + e);
      e.printStackTrace();
      subscription.forEach(Subscription::unsubscribe);
      System.exit(1);
    }
  }

  /**
   * Parses command line arguments and creates a Config object.
   *
   * The (optionally provided) config json provides the base. Its values are
   * overwritten with values provided on the command line. In the end, all
   * fields (except separator, which defaults to "~~") need to be set.
   *
   * @param args  command line argument list
   * @return  a new Config object
   */
  private static Config parseArgs(String[] args) {

    if (args.length == 0 || "help".equals(args[0]) || "-h".equals(args[0])) {
      System.out.println(HELP);
      System.exit(0);
      return new Config();  // unreachable
    } else {
      // prefix whitespace is added to keep regex simple
      final String paramString = " " + StringUtils.join(args, " ");
      final Matcher m = PARAMS_PATTERN.matcher(paramString);

      if (m.find()) {
        final Config config = option(m.group("conf")).fold(
            Config::new,
            conf -> {
              try {
                return new ObjectMapper().readValue(new File(conf), Config.class);
              } catch (IOException e) {
                LOG.warn("Error reading configuration file {}, using empty as base.", conf);
                return new Config();
              }
            });

        option(m.group("database")).forEach(config::setDatabase);
        option(m.group("user")).forEach(config::setUser);
        option(m.group("password")).forEach(config::setPassword);
        option(m.group("bucket")).forEach(config::setBucket);
        option(m.group("file")).forEach(config::setFile);
        option(m.group("separator")).forEach(config::setSeparator);
        option(m.group("query")).forEach(config::setQuery);

        return config;
      } else return new Config();
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
            } catch (SQLException e) {
              subscriber.onError(e);
            }
          }
        }
    );
  }

  private static class Config {
    private static final String DEFAULT_SEPARATOR = "~~";

    private String database;
    private String user;
    private String password;
    private String bucket;
    private String file;
    private String separator = DEFAULT_SEPARATOR;
    private String query;

    Config() {}

    List<String> validate() {
      final ImmutableList.Builder<String> errors = ImmutableList.builder();
      if (database == null) errors.add("No database specified; " + MISSING_PARAM);
      if (user == null) errors.add("No user specified; " + MISSING_PARAM);
      if (password == null) errors.add("No password specified; " + MISSING_PARAM);
      if (bucket == null) errors.add("No bucket specified; " + MISSING_PARAM);
      if (file == null) errors.add("No file specified; " + MISSING_PARAM);

      if (separator == null) separator = DEFAULT_SEPARATOR;
      else if (separator.isEmpty()) LOG.info("Column separator is empty");

      if (query == null) errors.add("No query specified; " + MISSING_PARAM);
      else if (!query.startsWith("SELECT")) errors.add("Query must start with 'SELECT'");

      return errors.build();
    }

    String getDatabase() {
      return database;
    }

    void setDatabase(String database) {
      this.database = database;
    }

    String getUser() {
      return user;
    }

    void setUser(String user) {
      this.user = user;
    }

    String getPassword() {
      return password;
    }

    void setPassword(String password) {
      this.password = password;
    }

    String getBucket() {
      return bucket;
    }

    void setBucket(String bucket) {
      this.bucket = bucket;
    }

    String getFile() {
      return file;
    }

    void setFile(String file) {
      this.file = file;
    }

    String getSeparator() {
      return separator;
    }

    void setSeparator(String separator) {
      this.separator = separator;
    }

    String getQuery() {
      return query;
    }

    void setQuery(String query) {
      this.query = query;
    }
  }
}
