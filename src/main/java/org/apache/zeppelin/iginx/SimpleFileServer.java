package org.apache.zeppelin.iginx;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.*;
import java.util.Enumeration;
import java.util.logging.FileHandler;

public class SimpleFileServer {

  public static String PREFIX = "/files";
  private int port;
  private String fileDir;

  protected static final boolean isOnWin =
      System.getProperty("os.name").toLowerCase().contains("win");

  private HttpServer httpServer = null;

  public SimpleFileServer(int port, String fileDir) {
    this.port = port;
    this.fileDir = fileDir;
  }

  public void start() throws IOException {
    // 检测端口是否被占用，如果占用则kill掉
    try {
      new Socket("localhost", port).close();
      if (isOnWin) {
        Runtime.getRuntime()
            .exec(
                "for /f \"tokens=5\" %a in ('netstat -ano ^| findstr :"
                    + port
                    + "') do taskkill /F /PID %a");
      } else {
        Runtime.getRuntime().exec("kill -9 $(lsof -t -i:" + port + ")");
      }
    } catch (IOException e) {
      // do nothing
    }
    httpServer = HttpServer.create(new InetSocketAddress(port), 0);
    httpServer.createContext(PREFIX, new FileHandler(fileDir));
    httpServer.start();
  }

  public void stop() {
    if (httpServer != null) {
      httpServer.stop(0);
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
