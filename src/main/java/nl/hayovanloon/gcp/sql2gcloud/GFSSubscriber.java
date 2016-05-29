package nl.hayovanloon.gcp.sql2gcloud;

import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Acl;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.collect.ImmutableList;
import io.atlassian.fugue.Option;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import rx.Subscriber;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class GFSSubscriber extends Subscriber<List<String>> {

  private static final Logger LOG = LogManager.getLogger(GFSSubscriber.class);

  // For now, always use default bucket ACLS
  private static final List<Acl> ACLS = ImmutableList.of();

  private final Connection connection;
  private final WriteChannel channel;
  private final String separator;
  private final Pattern sepRegex;

  private long start = System.currentTimeMillis();
  private long counter; // synchronised
  private long errors;  // synchronised

  private GFSSubscriber(Connection connection, WriteChannel channel, String separator, Pattern sepRegex) {
    this.connection = connection;
    this.channel = channel;
    this.separator = separator;
    this.sepRegex = sepRegex;
  }

  @Override
  public void onStart() {
    start = System.currentTimeMillis();
    super.onStart();
  }

  /**
   * Factor method for GFSSubscribers.
   *
   * @param connection  database connection
   * @param bucket      name of bucket to write to
   * @param fileName    name of file to write to
   * @param separator   column separating string
   * @return  a new GFSSubscriber
   */
  static GFSSubscriber of(Connection connection, String bucket, String fileName, String separator) {
    final Storage storage = StorageOptions.defaultInstance().service();
    final BlobInfo blobInfo = BlobInfo.builder(bucket, fileName).acl(ACLS).build();

    final WriteChannel channel = Option.option(storage.get(bucket, fileName)).fold(
        () -> storage.create(blobInfo).writer(),
        blob -> blob.writer());
    final Pattern sepRegex = Pattern.compile(Pattern.quote(separator));
    return new GFSSubscriber(connection, channel, separator, sepRegex);
  }

  @Override
  public void onCompleted() {
    closeConnections();

    final long num = getCounter();
    final long duration = (System.currentTimeMillis() - start);
    final long speed = Math.round(num / (duration/(double)1000));

    LOG.info("Job took {} seconds.", (duration / 1000));
    LOG.info("Wrote {} lines at {} lines/second.", num, speed);

    if (getErrors() > 0) LOG.warn("Encountered {} errors while writing.", getErrors());
    else LOG.info("Run has been successfully completed.");
  }

  @Override
  public void onError(Throwable t) {
    LOG.warn("Exception while reading: {}", t);
    closeConnections();
  }

  /**
   * Closes WriteChannel and database connection
   */
  private void closeConnections() {
    try {
      channel.close();
      connection.close();
    } catch (SQLException | IOException e) {
      LOG.error("Exception while closing connections/channels (possible loss of data): {}", e);
      System.exit(2);
    }
  }

  @Override
  public void onNext(List<String> xs) {
    final List<String> clean = xs.stream()
        .map(x -> sepRegex.matcher(x).replaceAll("").replace('\n', ' '))
        .collect(Collectors.toList());
    final String data = StringUtils.join(clean, separator) + "\n";
    final ByteBuffer byteBuffer = ByteBuffer.wrap(data.getBytes(StandardCharsets.UTF_8));

    try {
      channel.write(byteBuffer);
      inc();
    } catch (IOException e) {
      LOG.warn(e);
      incErrors();
    }
  }

  /**
   * Increment counter for successful writes
   */
  private synchronized void inc() {
    counter += 1;
  }

  private synchronized long getCounter() {
    return counter;
  }

  /**
   * Increment counter for unsuccessful writes
   */
  private synchronized void incErrors() {
    errors += 1;
  }

  private synchronized long getErrors() {
    return errors;
  }
}
