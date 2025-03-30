package org.apache.zeppelin.iginx.util;

import java.io.StringWriter;
import java.util.Properties;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;
import org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader;

public class VelocityUtil {
  private VelocityUtil() {}

  public static String generate(String resourceName, VelocityContext context) {
    Properties properties = new Properties();
    properties.setProperty(VelocityEngine.RESOURCE_LOADER, VelocityEngine.RESOURCE_LOADER_CLASS);
    properties.setProperty("class.resource.loader.class", ClasspathResourceLoader.class.getName());
    VelocityEngine velocityEngine = new VelocityEngine(properties);
    velocityEngine.init();
    StringWriter writer = new StringWriter();
    velocityEngine.mergeTemplate(resourceName, "UTF-8", context, writer);
    return writer.toString();
  }
}
