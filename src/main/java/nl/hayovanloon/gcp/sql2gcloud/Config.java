package nl.hayovanloon.gcp.sql2gcloud;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import io.atlassian.fugue.Option;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.atlassian.fugue.Option.option;

public class Config {

  private static final Logger LOG = LogManager.getLogger(Config.class);
  private static final Pattern PARAMS_PATTERN = Pattern.compile(
      "(" +
          "(\\s+-c\\s+(?<conf>[^\\s]+))" +
          "|(\\s+-dr\\s+(?<driver>[^\\s]+))" +
          "|(\\s+-d\\s+(?<database>jdbc:[^\\s]+))" +
          "|(\\s+-u\\s+(?<user>[^\\s]+))" +
          "|(\\s+-p\\s+\"(?<password>[^\"]+)\")" +
          "|(\\s+-s\\s+\"(?<separator>[^\"]+)\"))+" +
          "(\\s+gs://(?<bucket>[a-z0-9-_.]{3,63})/(?<file>[^\\s]+))?" +
          "(\\s+(?<query>.+))?");
  private static final String HELP =
      "Usage:" +
          "\n  sql2gcloud [-c <config>] [-d jdbc:<database>] [-u <user>] [-p \"<password>\"] [-s \"<separator>\"]" +
          "                 [-dr <jdbc driver>] [<dest_url>] [<query>]" +
          "\n" +
          "\nExamples:" +
          "\n  sql2gcloud -c conf/config.json gs://foo/bar/bla.txt SELECT \\* FROM foo" +
          "\n  sql2gcloud -c conf/config.json -u myuser -p \\\"mypass\\\" gs://foo/bar/bla.txt SELECT \\* FROM foo" +
          "\n" +
          "\nConfig file format (JSON):" +
          "\n{" +
          "\n  \"driver\": \"com.mysql.jdbc.Driver\"," +
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

  private static final String DEFAULT_SEPARATOR = "~~";

  private Option<String> driver = Option.none();
  private String database;
  private String user;
  private String password;
  private String bucket;
  private String file;
  private String separator = DEFAULT_SEPARATOR;
  private String query;

  Config() {}

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
  static Config parseArgs(String[] args) {

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

        option(m.group("driver")).forEach(config::setDriver);
        option(m.group("database")).forEach(config::setDatabase);
        option(m.group("user")).forEach(config::setUser);
        option(m.group("password")).forEach(config::setPassword);
        option(m.group("bucket")).forEach(config::setBucket);
        option(m.group("file")).forEach(config::setFile);
        option(m.group("separator")).forEach(config::setSeparator);
        option(m.group("query")).forEach(config::setQuery);

        System.out.println(config);
        return config;
      } else return new Config();
    }
  }

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

  Option<String> getDriver() {
    return driver;
  }

  public void setDriver(String driver) {
    this.driver = Option.some(driver);
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

  @Override
  public String toString() {
    return "Config{" +
        "driver=" + driver +
        ", database='" + database + '\'' +
        ", user='" + user + '\'' +
        ", password='" + password + '\'' +
        ", bucket='" + bucket + '\'' +
        ", file='" + file + '\'' +
        ", separator='" + separator + '\'' +
        ", query='" + query + '\'' +
        '}';
  }
}
