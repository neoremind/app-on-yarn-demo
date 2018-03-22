package com.neoremind.app.on.yarn.demo;

import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.Map;

public class NestoYarnHelper {
  private static final SimpleDateFormat formater = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss SSS zzz");

  public static String buildClassPathEnv(Configuration conf) {
    StringBuilder classPathEnv = new StringBuilder(ApplicationConstants.Environment.CLASSPATH.$$())
        .append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("$PWD/log4j.properties")
        .append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("$PWD")
        .append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("$HADOOP_CLIENT_CONF_DIR")
        .append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("$HADOOP_CONF_DIR")
        .append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("$JAVA_HOME/lib/tools.jar")
        .append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("$PWD/")
        .append(NestoYarnConstants.NESTO_YARN_FRAMEWORK_LINKEDNAME).append("/")
        .append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("$PWD/")
        .append(NestoYarnConstants.NESTO_YARN_FRAMEWORK_LINKEDNAME).append("/conf/")
        .append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("$PWD/")
        .append(NestoYarnConstants.NESTO_YARN_FRAMEWORK_LINKEDNAME).append("/*");
    for (String c : conf.getStrings(
        YarnConfiguration.YARN_APPLICATION_CLASSPATH,
        YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
      classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
      classPathEnv.append(c.trim());
    }
    return classPathEnv.toString();
  }

  public static void addFrameworkToDistributedCache(String framework_path,
                                                    Map<String, LocalResource> localResources, Configuration conf) throws IOException {
    URI uri;
    try {
      uri = new URI(framework_path);
    } catch(URISyntaxException e) {
      throw new IllegalArgumentException("Unable to parse '" + framework_path
          + "' as a URI.");
    }

    Path path = new Path(uri.getScheme(), uri.getAuthority(), uri.getPath());
    FileSystem fs = path.getFileSystem(conf);
    Path frameworkPath = fs.makeQualified(
        new Path(uri.getScheme(), uri.getAuthority(), uri.getPath()));

    FileContext fc = FileContext.getFileContext(frameworkPath.toUri(), conf);
    frameworkPath = fc.resolvePath(frameworkPath);
    uri = frameworkPath.toUri();
    try {
      uri = new URI(uri.getScheme(), uri.getAuthority(), uri.getPath(),
          null, NestoYarnConstants.NESTO_YARN_FRAMEWORK_LINKEDNAME);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(e);
    }

    FileStatus scFileStatus = fs.getFileStatus(frameworkPath);
    LocalResource scRsrc =
        LocalResource.newInstance(
            ConverterUtils.getYarnUrlFromURI(uri),
            LocalResourceType.ARCHIVE, LocalResourceVisibility.PRIVATE,
            scFileStatus.getLen(), scFileStatus.getModificationTime());
    localResources.put(NestoYarnConstants.NESTO_YARN_FRAMEWORK_LINKEDNAME, scRsrc);
  }

  public static boolean fileExist(String filePath) {
    return new File(filePath).exists();
  }

  public static void dumpOutDebugInfo(Log LOG) {
    Map<String, String> envs = System.getenv();
    for (Map.Entry<String, String> env : envs.entrySet()) {
      LOG.info("System env: key=" + env.getKey() + ", val=" + env.getValue());
      System.out.println("System env: key=" + env.getKey() + ", val="
          + env.getValue());
    }

    BufferedReader buf = null;
    try {
      String lines = Shell.WINDOWS ? Shell.execCommand("cmd", "/c", "dir") :
          Shell.execCommand("ls", "-al");
      buf = new BufferedReader(new StringReader(lines));
      String line = "";
      while ((line = buf.readLine()) != null) {
        LOG.info("System CWD content: " + line);
        System.out.println("System CWD content: " + line);
      }
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      IOUtils.cleanup(LOG, buf);
    }
  }

  public static String buildJmxJvmProperty() {
    StringBuilder builder = new StringBuilder();
    builder.append("-Djava.util.logging.config.file=$PWD/" + NestoYarnConstants.NESTO_YARN_FRAMEWORK_LINKEDNAME
        + "/conf/" + NestoYarnConstants.NESTO_YARN_APP_JMX);
    builder.append(" -Dcom.sun.management.jmxremote");
    builder.append(" -Dcom.sun.management.jmxremote.port=0");
    builder.append(" -Dcom.sun.management.jmxremote.local.only=false");
    builder.append(" -Dcom.sun.management.jmxremote.authenticate=false");
    builder.append(" -Dcom.sun.management.jmxremote.ssl=false");
    return builder.toString();
  }

  public static String getAppMasterLog4jFile() {
    return "./" + NestoYarnConstants.NESTO_YARN_FRAMEWORK_LINKEDNAME
        + "/conf/" + NestoYarnConstants.NESTO_YARN_APPMASTER_LOG4J;
  }

  public static String getAppContainerLog4jFile() {
    return "./" + NestoYarnConstants.NESTO_YARN_FRAMEWORK_LINKEDNAME
        + "/conf/" + NestoYarnConstants.NESTO_YARN_APPCONTAINER_LOG4J;
  }


  public static String getContainerLogAddress(Container container) {
    return "http://" + container.getNodeHttpAddress() + "/node/containerlogs/" + container.getId();
  }

  public static String getContainerKillAddress(Container container) {
    return "kill?containerId=" + container.getId().getContainerId();
  }

//  public static String generateDeadContainerTable(Collection<Container> containers) {
//    StringBuffer sb = new StringBuffer();
//    sb.append("<table border=\"1\" cellpadding=\"5\" cellspacing=\"0\" class=\"sortable\">\n");
//    sb.append("<tr>");
//    sb.append("<td><b>Container Id</b></td>");
//    sb.append("<td><b>Node Id</b></td>");
//    sb.append("<td><b>Node http</b></td>");
//    sb.append("<td><b>Resource</b></td>");
//    sb.append("<td><b>Log URL</b></td>");
//    sb.append("</tr>\n");
//    for (Container container : containers) {
//      sb.append("<tr><td>" + container.getId()
//              + "</td><td>" + container.getNodeId()
//              + "</td><td>" + container.getNodeHttpAddress()
//              + "</td><td>" + container.getResource()
//              + "</td><td>" + "<a href=\"" + getContainerLogAddress(container) + "\">logs"
//              + "</td></tr>\n");
//    }
//    sb.append("</table>\n");
//
//    return sb.toString();
//  }
//
//  public static String generateDeadNestoServerTable(Collection<Pair<NestoEndpoint, Long>> deadEndpoints) {
//    StringBuffer sb = new StringBuffer();
//    sb.append("<table border=\"1\" cellpadding=\"5\" cellspacing=\"0\" class=\"sortable\">\n");
//    sb.append("<tr>");
//    sb.append("<td><b>Server Id</b></td>");
//    sb.append("<td><b>Server StartTime</b></td>");
//    sb.append("<td><b>Server DieTime</b></td>");
//    sb.append("<td><b>Outer Port</b></td>");
//    sb.append("<td><b>Role</b></td>");
//    sb.append("<td><b>Http URL</b></td>");
//    sb.append("<td><b>Checksum</b></td>");
//    sb.append("</tr>\n");
//
//    for (Pair<NestoEndpoint, Long> entry : deadEndpoints) {
//      NestoEndpoint endpoint = entry.getLeft();
//      Long dieTime = entry.getRight();
//      String serverId = endpoint.host + "_" + endpoint.innerPort;
//      String httpAddress = endpoint.host + ":" + endpoint.httpPort;
//
//      sb.append("<tr><td>" + serverId
//              + "</td><td>" + formater.format(new Date(endpoint.startTime))
//              + "</td><td>" + formater.format(new Date(dieTime))
//              + "</td><td>" + endpoint.outerPort
//              + "</td><td>" + endpoint.role
//              + "</td><td>" + "<a href=\"http://" + httpAddress + "\">" + httpAddress
//              + "</td><td>" + endpoint.checksum
//              + "</td></tr>\n");
//    }
//    sb.append("</table>\n");
//
//    return sb.toString();
//  }
//
//  public static Map<String, NestoEndpoint> getEndpointsMap(Collection<NestoEndpoint> endpoints) {
//    Map<String, NestoEndpoint> m = Maps.newConcurrentMap();
//    for (NestoEndpoint endpoint : endpoints) {
//      m.put(endpoint.host, endpoint);
//    }
//    return m;
//  }
//
//  public static String generateNestoServerTable(Collection<NestoEndpoint> endpoints,
//                                                Map<ContainerId, Container> runningContainers) {
//    Map<String, NestoEndpoint> endpointsMap = getEndpointsMap(endpoints);
//
//    StringBuffer sb = new StringBuffer();
//    sb.append("<table border=\"1\" cellpadding=\"5\" cellspacing=\"0\" class=\"sortable\">\n");
//    sb.append("<tr>");
//    sb.append("<td><b>ContainerId</b></td>");
//    sb.append("<td><b>Server Id</b></td>");
//    sb.append("<td><b>Server StartTime</b></td>");
//    sb.append("<td><b>Outer Port</b></td>");
//    sb.append("<td><b>Role</b></td>");
//    sb.append("<td><b>Http URL</b></td>");
//    sb.append("<td><b>Log URL</b></td>");
//    sb.append("<td><b>Checksum</b></td>");
//    sb.append("<td><b>Action</b></td>");
//    sb.append("</tr>\n");
//    for (Container container : runningContainers.values()) {
//      NestoEndpoint endpoint = endpointsMap.get(container.getNodeId().getHost());
//      String serverId = "";
//      String httpAddress = "";
//      String startTime = "";
//      String role = "";
//      String checksum = "";
//      String outPort = "";
//      if (endpoint != null) {
//        serverId = endpoint.host + "_" + endpoint.innerPort;
//        String addr = endpoint.host + ":" + endpoint.httpPort;
//        httpAddress = "<a href=\"http://" + addr + "\">" + addr;
//        startTime = formater.format(new Date(endpoint.startTime));
//        role = endpoint.role.name();
//        outPort = String.valueOf(endpoint.getOuterPort());
//        checksum = endpoint.checksum;
//      }
//      sb.append("<tr><td>" + container.getId()
//              + "</td><td>" + serverId
//              + "</td><td>" + startTime
//              + "</td><td>" + outPort
//              + "</td><td>" + role
//              + "</td><td>" + httpAddress
//              + "</td><td>" + "<a href=\"" + getContainerLogAddress(container) + "\">logs"
//              + "</td><td>" + checksum
//              + "</td><td>" + "<a href=\"" + getContainerKillAddress(container) + "\">kill"
//              + "</td></tr>\n");
//    }
//    sb.append("</table>\n");
//
//    return sb.toString();
//  }

  public static void CheckConfPath(String confPath) {
//    final String[] configs = {"YarnAppMasterLog4j.properties", "YarnAppContainerLog4j.properties"};
    final String[] configs = {};
    for(String config: configs) {
      if(!Files.isRegularFile(Paths.get(confPath + "/" + config)))
        throw new RuntimeException("File " + config + " is not existed under directory " + confPath);
    }
  }
}