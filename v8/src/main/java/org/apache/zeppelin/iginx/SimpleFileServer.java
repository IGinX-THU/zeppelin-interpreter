package org.apache.zeppelin.iginx;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.io.*;
import java.net.*;
import java.nio.file.Files;
import java.util.*;
import org.apache.zeppelin.iginx.util.HttpUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleFileServer {
  private static final Logger LOGGER = LoggerFactory.getLogger(SimpleFileServer.class);

  public static String PREFIX = "/files";
  public static String PREFIX_UPLOAD = "/files/upload";
  public static String PREFIX_GRAPH = "/graphs";
  private int port;
  private String fileDir;
  private String uploadFileDir;
  private Long uploadDirMaxSize;
  protected static final boolean isOnWin =
      System.getProperty("os.name").toLowerCase().contains("win");

  private HttpServer httpServer = null;

  public SimpleFileServer(int port, String fileDir, String uploadFileDir, long uploadDirMaxSize) {
    this.port = port;
    this.fileDir = fileDir;
    this.uploadFileDir = uploadFileDir;
    this.uploadDirMaxSize = uploadDirMaxSize;
  }

  public void start() throws IOException {
    try (ServerSocket socket = new ServerSocket(port)) {
      socket.setReuseAddress(true);
    } catch (IOException e) {
      LOGGER.error("Port {} is already in use, trying to kill the process...", port);

      if (isOnWin) {
        killProcessOnWindows(port);
      }
    }

    try {
      LOGGER.info("Starting SimpleFileServer on port " + port);
      httpServer = HttpServer.create(new InetSocketAddress(port), 0);
      httpServer.createContext(PREFIX, new FileHandler(fileDir));
      httpServer.createContext(PREFIX_UPLOAD, new UploadHandler(uploadFileDir));
      httpServer.createContext(PREFIX_GRAPH, new GraphHandler(fileDir));
      httpServer.start();
    } catch (IOException e) {
      LOGGER.error("Error starting SimpleFileServer", e);
      throw new RuntimeException(e);
    }
  }

  public void stop() {
    if (httpServer != null) {
      httpServer.stop(0);
    }
  }

  private void killProcessOnWindows(int port) {
    try {
      // 执行 netstat 命令查找占用端口的进程 ID
      String cmd = "netstat -ano | findstr :" + port;
      ProcessBuilder builder = new ProcessBuilder("cmd.exe", "/c", cmd);
      Process process = builder.start();
      try (BufferedReader reader =
          new BufferedReader(new InputStreamReader(process.getInputStream()))) {
        String line;
        Set<String> pids = new HashSet<>();
        while ((line = reader.readLine()) != null) {
          LOGGER.info("netstat output: {}", line);
          String[] parts = line.split("\\s+");
          if (parts.length > 4) {
            String pid = parts[parts.length - 1]; // PID 是 netstat 输出的最后一个字段
            pids.add(pid);
          }
        }
        if (pids.isEmpty()) {
          LOGGER.warn("No process found occupying port {}", port);
          return;
        }
        // 遍历所有 PID 并使用 taskkill 命令终止进程
        for (String pid : pids) {
          String killCmd = "taskkill /F /PID " + pid;
          process = Runtime.getRuntime().exec(killCmd);
          int exitCode = process.waitFor(); // 等待命令执行完成
          if (exitCode == 0) {
            LOGGER.info("Successfully killed process(pid is {}) occupying port {}", pid, port);
          } else {
            LOGGER.error("Failed to kill process(pid is {}) on port {}", pid, port);
          }
        }
      }
    } catch (Exception ex) {
      LOGGER.error("Failed to kill process on port {}", port, ex);
    }
  }

  static class FileHandler implements HttpHandler {
    private String basePath;

    public FileHandler(String basePath) {
      this.basePath = basePath;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
      try {
        // 添加 CORS 响应头
        exchange.getResponseHeaders().add("Access-Control-Allow-Origin", "*");

        // 获取请求的文件名，并构建文件路径
        String requestPath = exchange.getRequestURI().getPath();
        String fileName = requestPath.substring(PREFIX.length());
        File file = new File(basePath + fileName);

        // 检查文件是否存在且不是目录
        if (file.exists() && !file.isDirectory()) {
          // 设置响应头为文件下载
          exchange.getResponseHeaders().set("Content-Type", "application/octet-stream");
          exchange
              .getResponseHeaders()
              .set("Content-Disposition", "attachment; filename=\"" + file.getName() + "\"");
          exchange.sendResponseHeaders(200, file.length());

          // 读取文件并写入响应体
          OutputStream os = exchange.getResponseBody();
          FileInputStream fs = new FileInputStream(file);
          final byte[] buffer = new byte[0x10000];
          int count = 0;
          while ((count = fs.read(buffer)) >= 0) {
            os.write(buffer, 0, count);
          }
          fs.close();
          os.close();
        } else {
          // 如果文件不存在，返回404错误，响应体为"404 (Not Found)，可能文件已被删除，请重新执行查询“
          String response = "404 (Not Found)，可能文件已被删除，请重新执行查询";
          exchange.sendResponseHeaders(404, response.length());
          OutputStream os = exchange.getResponseBody();
          os.write(response.getBytes());
          os.close();
        }
      } catch (IOException e) {
        e.printStackTrace();
        exchange.sendResponseHeaders(500, 0); // 发送500错误
        exchange.getResponseBody().close();
      }
    }
  }

  /** upload csv file handler */
  class UploadHandler implements HttpHandler {
    private final String basePath;

    public UploadHandler(String basePath) {
      this.basePath = basePath;
    }

    @Override
    public void handle(HttpExchange exchange) {

      String zeppelinUrl = "", noteBookId = "", paragraphId = "", fileName = "";
      BufferedWriter bw = null;
      BufferedReader br = null;
      String line;
      boolean isContent = false;
      try {
        if (!"POST".equals(exchange.getRequestMethod())) {
          exchange.sendResponseHeaders(405, -1);
          return;
        }
        exchange.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
        File uploadDir = new File(HttpUtil.getCurrentPath(basePath));
        if (!uploadDir.exists()) {
          uploadDir.mkdirs();
        }
        br =
            new BufferedReader(
                new InputStreamReader(new BufferedInputStream(exchange.getRequestBody())));
        /* parse form-data */
        while ((line = br.readLine()) != null) {
          LOGGER.debug(line);
          if (isContent) {
            if (line.trim().isEmpty()) {
              break;
            }
            bw.write(line);
            bw.newLine();
          }
          if (line.startsWith("------")) {
            line = br.readLine();
            if (line.contains("filename=")) {
              fileName = line.substring(line.indexOf("filename=") + 10, line.length() - 1);
              br.readLine();
              br.readLine();
              isContent = true;
              bw =
                  new BufferedWriter(
                      new OutputStreamWriter(
                          Files.newOutputStream(new File(uploadDir, fileName).toPath())));
            } else {
              String paramName = line.substring(line.indexOf("name=") + 6, line.length() - 1);
              br.readLine();
              switch (paramName) {
                case "zeppelinUrl":
                  zeppelinUrl = br.readLine().trim();
                  break;
                case "noteBookId":
                  noteBookId = br.readLine().trim();
                  break;
                case "paragraphId":
                  paragraphId = br.readLine().trim();
                  break;
                default:
                  LOGGER.warn("unexpected params received {}", br.readLine().trim());
                  break;
              }
            }
          }
        }
        if (bw != null) {
          bw.close();
        }
        LOGGER.info(
            "received parameters:{},{},{},{}", zeppelinUrl, noteBookId, paragraphId, fileName);
        String result =
            HttpUtil.sendPost(
                String.format("%s/api/notebook/run/%s/%s", zeppelinUrl, noteBookId, paragraphId),
                null);
        LOGGER.info("result of rerun paragraph command: {}", result);
        exchange.sendResponseHeaders(200, 0);
      } catch (IOException e) {
        LOGGER.error("Error uploading file", e);
        throw new RuntimeException(e);
      } finally {
        exchange.close();
        cleanUpLoadDir();
      }
    }
  }

  static class GraphHandler implements HttpHandler {
    private String basePath;

    public GraphHandler(String basePath) {
      this.basePath = basePath;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
      exchange.getResponseHeaders().add("Access-Control-Allow-Origin", "*");

      // 获取请求的文件名，并构建文件路径
      String requestPath = exchange.getRequestURI().getPath();
      File file = new File(basePath + requestPath);
      if (file.exists() && !file.isDirectory()) {
        // 设置响应头为文件下载
        if (requestPath.endsWith("html")) {
          exchange.getResponseHeaders().set("Content-Type", "text/html");
        } else {
          exchange.getResponseHeaders().set("Content-Type", "text/plain");
        }
        exchange.sendResponseHeaders(200, file.length());

        // 读取文件并写入响应体
        OutputStream os = exchange.getResponseBody();
        FileInputStream fs = new FileInputStream(file);
        final byte[] buffer = new byte[0x10000];
        int count = 0;
        while ((count = fs.read(buffer)) >= 0) {
          os.write(buffer, 0, count);
        }
        fs.close();
        os.close();
      } else {
        String responseMeg = "404 (Not Found)，可能文件已被删除，请重新执行查询";
        exchange.sendResponseHeaders(404, responseMeg.length());
        Writer response = new OutputStreamWriter(exchange.getResponseBody());
        response.write(responseMeg);
        response.close();
      }
    }
  }
  /** clean earliest files when upload file director exceeds 100GB */
  public void cleanUpLoadDir() {
    new Thread(
            () -> {
              File directory = new File(uploadFileDir);
              File[] files = directory.listFiles();
              if (files != null) {
                // order by lastModified -  asc
                Arrays.sort(files, Comparator.comparingLong(File::lastModified));
                long totalSize = Arrays.stream(files).mapToLong(File::length).sum();
                for (File file : files) {
                  if (totalSize <= uploadDirMaxSize) {
                    break;
                  }
                  LOGGER.info("Deleted file {},{}", file.getAbsolutePath(), file.length());
                  totalSize = totalSize - file.length();
                  file.delete();
                }
              }
            })
        .start();
  }

  /**
   * 获取本地主机地址，普通方法会获取到回环地址or错误网卡地址，因此需要使用更复杂的方法获取
   *
   * @return InetAddress 本机地址
   */
  public static String getLocalHostExactAddress() {
    try {
      Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
      while (networkInterfaces.hasMoreElements()) {
        NetworkInterface networkInterface = networkInterfaces.nextElement();
        if (networkInterface.isLoopback()
            || networkInterface.isVirtual()
            || !networkInterface.isUp()) {
          continue;
        }

        Enumeration<InetAddress> inetAddresses = networkInterface.getInetAddresses();
        while (inetAddresses.hasMoreElements()) {
          InetAddress inetAddress = inetAddresses.nextElement();
          if (!inetAddress.isLoopbackAddress()
              && !isPrivateIPAddress(inetAddress.getHostAddress())
              && inetAddress instanceof Inet4Address) {
            // 这里得到了非回环地址的IPv4地址
            return inetAddress.getHostAddress();
          }
        }
      }
    } catch (SocketException e) {
      e.printStackTrace();
    }
    return null;
  }

  // 判断是否为私有IP地址
  private static boolean isPrivateIPAddress(String ipAddress) {
    return ipAddress.startsWith("10.")
        || ipAddress.startsWith("192.168.")
        || (ipAddress.startsWith("172.")
            && (Integer.parseInt(ipAddress.split("\\.")[1]) >= 16
                && Integer.parseInt(ipAddress.split("\\.")[1]) <= 31));
  }
}
