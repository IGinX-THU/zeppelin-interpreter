package cn.edu.tsinghua;

import cn.edu.tsinghua.iginx.exception.SessionException;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.thrift.DataType;
import com.google.common.collect.Lists;

import java.io.*;
import java.util.*;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.zip.GZIPInputStream;

import me.tongfei.progressbar.ProgressBar;
import me.tongfei.progressbar.ProgressBarBuilder;

public class SchemaPileDataset {

  public static final String FILENAME = "schemapile-perm.json.gz";
  public static final String IGINX_HOST = System.getProperty("iginx.host", "127.0.0.1");
  public static final int IGINX_PORT = Integer.getInteger("iginx.port", 6888);
  public static final int BATCH_SIZE = 1000;

  protected static final List<Session> allSessions = new ArrayList<>();
  protected static final BlockingDeque<Session> sessionPool = new LinkedBlockingDeque<>();

  private static class SessionHandler implements AutoCloseable {
    Session session;

    public SessionHandler() {
      try {
        session = sessionPool.take();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void close() {
      sessionPool.offer(session);
    }
  }

  public static void main(String[] args) throws Exception {
    try {
      for(int i=0;i<16;i++){
        Session session = new Session(IGINX_HOST, IGINX_PORT);
        allSessions.add(session);
        sessionPool.offer(session);
        session.openSession();
      }
      System.out.println("Preparing to insert data...");
      SchemaPileDataset dataset = new SchemaPileDataset();
      dataset.insertInto(SessionHandler::new);
    }finally {
      Exception exception = null;
      for (Session session : allSessions) {
        try {
          session.closeSession();
        }catch (SessionException e){
          if(exception==null){
            exception=e;
          }else{
            exception.addSuppressed(e);
          }
        }
      }
    }
  }

  protected final List<IginxColumn> data;

  public SchemaPileDataset() throws IOException {
    String json = readGzipFileFromResources(FILENAME);
    SchemaPile schemaPile = new SchemaPile(json);
    this.data = schemaPile.toIginxColumn();
  }

  private static String readGzipFileFromResources(String filename) throws IOException {
    ClassLoader classLoader = SchemaPileDataset.class.getClassLoader();
    try (InputStream resourceStream = classLoader.getResourceAsStream(filename);
         GZIPInputStream gzip = new GZIPInputStream(resourceStream);
         InputStreamReader reader = new InputStreamReader(gzip);
         BufferedReader in = new BufferedReader(reader)) {
      StringBuilder sb = new StringBuilder();
      String line;
      while ((line = in.readLine()) != null) {
        sb.append(line);
      }
      return sb.toString();
    }
  }

  private void insertInto(Supplier<SessionHandler> sessionFactory) {
    List<List<IginxColumn>> partitions = Lists.partition(data, BATCH_SIZE);
    try (ProgressBar progressBar =
             new ProgressBarBuilder()
                 .setTaskName("Inserting Data")
                 .setInitialMax(partitions.size())
                 .build()) {
      partitions
          .parallelStream()
          .forEach(
              partition -> {
                try {
                  insertDataInto(sessionFactory, partition);
                  progressBar.step();
                } catch (SessionException e) {
                  throw new RuntimeException(e);
                }
              });
    }
  }

  private static void insertDataInto(Supplier<SessionHandler> sessionFactory, List<IginxColumn> data)
      throws SessionException {

    int maxLen = data.stream().mapToInt(column -> column.getValues().length).max().orElse(0);
    long[] keys = IntStream.range(0, maxLen).mapToLong(i -> i).toArray();
    List<String> paths = new ArrayList<>();
    List<DataType> types = new ArrayList<>();
    List<Object[]> values = new ArrayList<>();

    for (IginxColumn column : data) {
      if (column.getPath().contains("shadowsocks")) {
        continue;
      }
      paths.add(column.getPath());
      types.add(column.getType());
      values.add(Arrays.copyOf(column.getValues(), maxLen));
    }

    try(SessionHandler sessionHandler=sessionFactory.get()) {
      sessionHandler.session.insertColumnRecords(paths, keys, values.toArray(), types);
    }
  }
}
