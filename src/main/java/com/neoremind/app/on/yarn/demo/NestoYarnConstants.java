package com.neoremind.app.on.yarn.demo;

public class NestoYarnConstants {
  public static final String NESTO_YARN_APPMASTER_MAINCLASS = "com.neoremind.app.on.yarn.demo.NestoYarnAppMaster";
  public static final String NESTO_YARN_SERVER_MAINCLASS = "com.neoremind.app.on.yarn.demo.NestoServerExecutor";

  public static final String NESTO_YARN_FRAMEWORK_LINKEDNAME = "nesto";

  public static final String NESTO_YARN_FRAMEWORK_PATH = "NESTO_YARN_FRAMEWORK_PATH";
  public static final int NESTO_YARN_APPMASTER_PRIORITY = -100;
  public static final int NESTO_YARN_SERVER_PRIORITY = -99;

  public static final String NESTO_YARN_JSP_ATTRIBUTE_NAME = "nestoyarn";

  public static final String NESTO_YARN_APPMASTER_LOG = "AppMaster.log";
  public static final String NESTO_YARN_APPCONTAINER_LOG = "AppContainer.log";

  public static final String NESTO_YARN_APPMASTER_LOG4J = "YarnAppMasterLog4j.properties";
  public static final String NESTO_YARN_APPCONTAINER_LOG4J = "YarnAppContainerLog4j.properties";
  public static final String NESTO_YARN_APP_JMX = "jmx.properties";

}