package com.neoremind.app.on.yarn.demo;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.apache.commons.cli.*;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

/**
 * Modified from DistributedShell Client in YARN Example
 */
public class NestoYarnClient {

    private static final Log LOG = LogFactory.getLog(NestoYarnClient.class);

    private Configuration conf;

    private YarnClient yarnClient;
    /**
     * Application master specific info to register a new Application with RM/ASM
     */
    private String appName = "";

    /**
     * Queue for App master
     */
    private String amQueue = "";

    /**
     * Amt. of memory resource to request for to run the App Master
     */
    private int amMemory = 1;

    /**
     * Amt. of virtual core resource to request for to run the App Master
     */
    private int amVCores = 1;

    /**
     * Application master jar file
     */
    private String frameworkPath = "";

    /**
     * Main class to invoke application master
     */
    private final String appMasterMainClass;

    /**
     * Args to be passed to the NestoServer
     */
    private String[] nestoServerArgs = new String[]{};

    /**
     * Env variables to be setup for the shell command
     */
    private Map<String, String> nestoServerEnv = new HashMap<String, String>();

    /**
     * Amt of memory to request for container in which shell script will be executed
     */
    private int containerMemory = 10;

    private int coordMemory = 10;

    /**
     * Amt. of virtual cores to request for container in which shell script will be executed
     */
    private int containerVirtualCores = 1;

    /**
     * No. of containers in which the shell script needs to be executed
     */
    private int numContainers = 1;
    private int numCoords = 1;
    private String coordOpts = null;
    private String nodeLabelExpression = null;

    // in MB
    private int memoryOverhead = 384;

    // log4j.properties file
    // if available, add to local resources and set into classpath
    private String log4jPropFile = "";

    // Debug flag
    boolean debugFlag = false;
    boolean autoexit = false;

    // Command line options
    private Options opts;

    private boolean enableJMX = false;

    // Hardcoded path to custom log_properties
    private static final String log4jPath = "log4j.properties";

    private String confPath;

    /**
     * @param args Command line arguments
     */
    public static void main(String[] args) {
        boolean result = false;
        try {
            NestoYarnClient client = new NestoYarnClient();
            LOG.info("Initializing Client");
            try {
                boolean doRun = client.init(args);
                if (!doRun) {
                    System.exit(0);
                }
            } catch (IllegalArgumentException e) {
                System.err.println(e.getLocalizedMessage());
                client.printUsage();
                System.exit(-1);
            }
            result = client.run();
        } catch (Throwable t) {
            LOG.fatal("Error running Client", t);
            System.exit(1);
        }
        if (result) {
            LOG.info("Application completed successfully");
            System.exit(0);
        }
        LOG.error("Application failed to complete successfully");
        System.exit(2);
    }

    public NestoYarnClient(Configuration conf) throws Exception {
        this(NestoYarnConstants.NESTO_YARN_APPMASTER_MAINCLASS, conf);
    }

    NestoYarnClient(String appMasterMainClass, Configuration conf) {
        this.conf = conf;
        this.appMasterMainClass = appMasterMainClass;
        yarnClient = YarnClient.createYarnClient();
        yarnClient.init(conf);
        opts = new Options();
        opts.addOption("appname", true, "Application Name. Default value - Nesto");
        opts.addOption("queue", true, "RM Queue in which this application is to be submitted");
        opts.addOption("master_memory", true, "Amount of memory in MB to be requested to run the application master");
        opts.addOption("master_vcores", true, "Amount of virtual cores to be requested to run the application master");
        opts.addOption("memory_overhead", true, "Amount of memory overhead in MB for application master and container");
        opts.addOption("framework_path", true, "Framework hdfs location containing the application master jar and conf");
        opts.addOption("env", true, "Environment for shell script. Specified as env_key=env_val pairs");
        opts.addOption("container_memory", true, "Amount of memory in MB to be requested to run server in container");
        opts.addOption("container_vcores", true, "Amount of virtual cores to be requested to run server in container");
        opts.addOption("num_containers", true, "No. of containers on which the shell command needs to be executed");
        opts.addOption("num_coords", true, "No. of containers on which run the nesto coord");
        opts.addOption("coord_memory", true, "Amount of memory in MB to be requested to run nesto coord in container");
        opts.addOption("coord_opts", true, "Java opts for starting coord");
        opts.addOption("log_properties", true, "log4j.properties file");
        opts.addOption("conf_path", true, "configuration location, e.g. /nesto/conf, should include nesto-site.xml," +
                "YarnAppMasterLog4j.properties and YarnAppContainerLog4j.properties");
        opts.addOption("debug", false, "Dump out debug information");
        opts.addOption("help", false, "Print usage");
        opts.addOption("enable_jmx", false, "Enable JMX");
        opts.addOption("autoexit", false, "Auto exit after successfully running.");
        opts.addOption("node_label_expression", true,
                "Node label expression to determine the nodes"
                        + " where all the containers of this application"
                        + " will be allocated, \"\" means containers"
                        + " can be allocated anywhere, if you don't specify the option,"
                        + " default node_label_expression of queue will be used.");
    }

    public NestoYarnClient() throws Exception {
        this(new YarnConfiguration());
    }

    /**
     * Helper function to print out usage
     */
    private void printUsage() {
        new HelpFormatter().printHelp("Client", opts);
    }

    /**
     * Parse command line options
     *
     * @param args Parsed command line options
     * @return Whether the init was successful to run the client
     * @throws ParseException
     */
    public boolean init(String[] args) throws ParseException {
        CommandLine cliParser = new GnuParser().parse(opts, args);

        if (args.length == 0) {
            throw new IllegalArgumentException("No args specified for client to initialize");
        }

        if (!cliParser.hasOption("conf_path")) {
            throw new IllegalArgumentException("--conf_path should be set. e.g: /nesto/conf/");
        }

        confPath = cliParser.getOptionValue("conf_path");
        NestoYarnHelper.CheckConfPath(confPath);

        if (cliParser.hasOption("log_properties")) {
            String log4jPath = cliParser.getOptionValue("log_properties");
            try {
                Log4jPropertyHelper.updateLog4jConfiguration(NestoYarnClient.class, log4jPath);
            } catch (Exception e) {
                LOG.warn("Can not set up custom log4j properties. " + e);
            }
        }

        if (cliParser.hasOption("help")) {
            printUsage();
            return false;
        }

        if (cliParser.hasOption("debug")) {
            debugFlag = true;
        }

        if (cliParser.hasOption("autoexit")) {
            autoexit = true;
        }

        if (cliParser.hasOption("enable_jmx")) {
            enableJMX = true;
        }

        appName = cliParser.getOptionValue("appname", "Nesto");
        amQueue = cliParser.getOptionValue("queue", "default");
        amMemory = Integer.parseInt(cliParser.getOptionValue("master_memory", "10"));
        amVCores = Integer.parseInt(cliParser.getOptionValue("master_vcores", "1"));

        if (amMemory < 0) {
            throw new IllegalArgumentException("Invalid memory specified for application master, exiting."
                    + " Specified memory=" + amMemory);
        }
        if (amVCores < 0) {
            throw new IllegalArgumentException("Invalid virtual cores specified for application master, exiting."
                    + " Specified virtual cores=" + amVCores);
        }

        if (!cliParser.hasOption("framework_path")) {
            throw new IllegalArgumentException("No archive file specified for framework.");
        }

        frameworkPath = cliParser.getOptionValue("framework_path");

        if (cliParser.hasOption("shell_args")) {
            nestoServerArgs = cliParser.getOptionValues("shell_args");
        }
        if (cliParser.hasOption("shell_env")) {
            String envs[] = cliParser.getOptionValues("shell_env");
            for (String env : envs) {
                env = env.trim();
                int index = env.indexOf('=');
                if (index == -1) {
                    nestoServerEnv.put(env, "");
                    continue;
                }
                String key = env.substring(0, index);
                String val = "";
                if (index < (env.length() - 1)) {
                    val = env.substring(index + 1);
                }
                nestoServerEnv.put(key, val);
            }
        }

        containerMemory = Integer.parseInt(cliParser.getOptionValue("container_memory", "10"));
        containerVirtualCores = Integer.parseInt(cliParser.getOptionValue("container_vcores", "1"));
        numContainers = Integer.parseInt(cliParser.getOptionValue("num_containers", "1"));
        coordMemory = Integer.parseInt(cliParser.getOptionValue("coord_memory", String.valueOf(containerMemory)));
        int defaultCoordNum = Math.min(Math.max((int) Math.ceil(numContainers * 0.2f), 1), 5);
        numCoords = Integer.parseInt(cliParser.getOptionValue("num_coords", String.valueOf(defaultCoordNum)));
        coordOpts = cliParser.getOptionValue("coord_opts");

        if (!cliParser.hasOption("memory_overhead")) {
            memoryOverhead = Math.max((int) (containerMemory * 0.1), 384);
        } else
            memoryOverhead = Integer.parseInt(cliParser.getOptionValue("memory_overhead", "384"));

        if (containerMemory < 0 || containerVirtualCores < 0 || numContainers < 1
                || coordMemory < 0 || numCoords < 1 || numCoords > numContainers) {
            throw new IllegalArgumentException("Invalid no. of containers or container memory/vcores specified,"
                    + " exiting."
                    + " Specified containerMemory=" + containerMemory
                    + ", coordMemory=" + coordMemory
                    + ", containerVirtualCores=" + containerVirtualCores
                    + ", numContainer=" + numContainers
                    + ", numCoords=" + numCoords);
        }

        nodeLabelExpression = cliParser.getOptionValue("node_label_expression", null);

        log4jPropFile = cliParser.getOptionValue("log_properties", "");

        return true;
    }

    /**
     * Main run function for the client
     *
     * @return true if application completed successfully
     * @throws IOException
     * @throws YarnException
     */
    public boolean run() throws IOException, YarnException {

        LOG.info("Running Client");
        yarnClient.start();

        int livedNodesNumber = yarnClient.getNodeReports(NodeState.RUNNING).size();
        if (numContainers > livedNodesNumber) {
            throw new IllegalArgumentException("Invalid numContainers specified for application, exiting."
                    + " Specified numContainers=" + numContainers + ", but only have:" + livedNodesNumber + " lived nodes.");
        }

        YarnClusterMetrics clusterMetrics = yarnClient.getYarnClusterMetrics();
        LOG.info("Got Cluster metric info from ASM"
                + ", numNodeManagers=" + clusterMetrics.getNumNodeManagers());

        QueueInfo queueInfo = yarnClient.getQueueInfo(this.amQueue);
        LOG.info("Queue info"
                + ", queueName=" + queueInfo.getQueueName()
                + ", queueCurrentCapacity=" + queueInfo.getCurrentCapacity()
                + ", queueMaxCapacity=" + queueInfo.getMaximumCapacity()
                + ", queueApplicationCount=" + queueInfo.getApplications().size()
                + ", queueChildQueueCount=" + queueInfo.getChildQueues().size());

        // Get a new application id
        YarnClientApplication app = yarnClient.createApplication();
        GetNewApplicationResponse appResponse = app.getNewApplicationResponse();
        // If we do not have min/max, we may not be able to correctly request
        // the required resources from the RM for the app master
        // Memory ask has to be a multiple of min and less than max.
        // Dump out information about cluster capability as seen by the resource manager
        int maxMem = appResponse.getMaximumResourceCapability().getMemory();
        LOG.info("Max mem capabililty of resources in this cluster " + maxMem);

        // A resource ask cannot exceed the max.
        if (amMemory + memoryOverhead > maxMem) {
            LOG.info("AM memory specified above max threshold of cluster. Using max value."
                    + ", specified=" + amMemory
                    + ", max=" + maxMem);
            amMemory = maxMem - memoryOverhead;
        }

        int maxVCores = appResponse.getMaximumResourceCapability().getVirtualCores();
        LOG.info("Max virtual cores capabililty of resources in this cluster " + maxVCores);

        if (amVCores > maxVCores) {
            LOG.info("AM virtual cores specified above max threshold of cluster. "
                    + "Using max value." + ", specified=" + amVCores
                    + ", max=" + maxVCores);
            amVCores = maxVCores;
        }

        // set the application name
        ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
        ApplicationId appId = appContext.getApplicationId();

        appContext.setKeepContainersAcrossApplicationAttempts(true);
        appContext.setApplicationName(appName);

        appContext.setAttemptFailuresValidityInterval(60000);
        appContext.setMaxAppAttempts(1000);

        if (debugFlag) {
            appContext.setAttemptFailuresValidityInterval(-1);
            appContext.setMaxAppAttempts(2);
        }

        // set local resources for the application master
        // local files or archives as needed
        // In this scenario, the jar file for the application master is part of the local resources
        Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

        LOG.info("Copy app archive from local filesystem and add to local environment");
        // Copy the application archive to the filesystem
        FileSystem fs = FileSystem.get(conf);
        NestoYarnHelper.addFrameworkToDistributedCache(frameworkPath, localResources, conf);

        // Set the log4j properties if needed
        if (!log4jPropFile.isEmpty()) {
            addToLocalResources(fs, log4jPropFile, log4jPath, appId.toString(),
                    localResources, null);
        }

        // Set the env variables to be setup in the env where the application master will be run
        LOG.info("Set the environment for the application master");
        Map<String, String> env = new HashMap<String, String>();

        env.put("CLASSPATH", NestoYarnHelper.buildClassPathEnv(conf));
        env.put(NestoYarnConstants.NESTO_YARN_FRAMEWORK_PATH, frameworkPath);

        // Set the necessary command to execute the application master
        Vector<CharSequence> vargs = new Vector<CharSequence>(30);

        // Print info for debug
        if (debugFlag) {
            vargs.add("cat $PWD/launch_container.sh > /tmp/yarn_${CONTAINER_ID}_launch_container.sh");
            vargs.add(" && ");
            vargs.add("ls -la $PWD/ > /tmp/yarn_${CONTAINER_ID}_pwd_dirs.txt");
            vargs.add(" && ");
        }
        // Set java executable command
        LOG.info("Setting up app master command");
        String log4jFile = confPath + "/" + NestoYarnConstants.NESTO_YARN_APPMASTER_LOG4J;
        try {
            String log4jContents = Files.toString(new File(log4jFile),
                    Charsets.UTF_8);
            vargs.add("echo '" + log4jContents + "' >$PWD/" + NestoYarnConstants.NESTO_YARN_APPMASTER_LOG4J);
            vargs.add(" && ");
        } catch (Exception e) {
            throw new RuntimeException("log4j file not existed:" + log4jFile, e);
        }

        vargs.add(ApplicationConstants.Environment.JAVA_HOME.$$() + "/bin/java");
        // Set Xms Xmx based on am memory size
        vargs.add("-Xms" + amMemory + "m");
        vargs.add("-Xmx" + amMemory + "m");
        vargs.add("-Djava.io.tmpdir=$PWD/tmp");
        vargs.add("-Dlog4j.configuration=" + NestoYarnConstants.NESTO_YARN_APPMASTER_LOG4J);

        if (enableJMX) {
            vargs.add(NestoYarnHelper.buildJmxJvmProperty());
        }

        // Set class name
        vargs.add(appMasterMainClass);
        // Set params for Application Master
        vargs.add("--container_memory " + String.valueOf(containerMemory));
        vargs.add("--coord_memory " + String.valueOf(coordMemory));
        vargs.add("--container_vcores " + String.valueOf(containerVirtualCores));
        vargs.add("--num_containers " + String.valueOf(numContainers));
        vargs.add("--num_coords " + String.valueOf(numCoords));
        vargs.add("--memory_overhead " + String.valueOf(memoryOverhead));
        if (coordOpts != null) {
            vargs.add("--coord_opts '" + coordOpts + "'");
        }
        if (enableJMX) {
            vargs.add("--enable_jmx ");
        }

        if (null != nodeLabelExpression) {
            appContext.setNodeLabelExpression(nodeLabelExpression);
            vargs.add("--node_label_expression " + nodeLabelExpression);
        }

        for (Map.Entry<String, String> entry : nestoServerEnv.entrySet()) {
            vargs.add("--shell_env " + entry.getKey() + "=" + entry.getValue());
        }
        if (debugFlag) {
            vargs.add("--debug");
        }

        vargs.add("1>>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/"
                + NestoYarnConstants.NESTO_YARN_APPMASTER_LOG);
        vargs.add("2>>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/"
                + NestoYarnConstants.NESTO_YARN_APPMASTER_LOG);

        // Get final commmand
        StringBuilder command = new StringBuilder();
        for (CharSequence str : vargs) {
            command.append(str).append(" ");
        }

        LOG.info("Completed setting up app master command " + command.toString());
        List<String> commands = new ArrayList<String>();
        commands.add(command.toString());

        // Set up the container launch context for the application master
        ContainerLaunchContext amContainer = ContainerLaunchContext.newInstance(
                localResources, env, commands, null, null, null);

        // Set up resource type requirements
        // For now, both memory and vcores are supported, so we set memory and
        // vcores requirements
        Resource capability = Resource.newInstance(amMemory + memoryOverhead, amVCores);
        appContext.setResource(capability);

        appContext.setAMContainerSpec(amContainer);

        // Set the priority for the application master
        Priority pri = Priority.newInstance(NestoYarnConstants.NESTO_YARN_APPMASTER_PRIORITY);
        appContext.setPriority(pri);

        // Set the queue to which this application is to be submitted in the RM
        appContext.setQueue(amQueue);
        appContext.setApplicationType("nesto");

        // Submit the application to the applications manager
        // SubmitApplicationResponse submitResp = applicationsManager.submitApplication(appRequest);
        // Ignore the response as either a valid response object is returned on success
        // or an exception thrown to denote some form of a failure
        LOG.info("Submitting application to ASM");

        yarnClient.submitApplication(appContext);

        // Monitor the application
        return monitorApplication(appId);

    }

    /**
     * Monitor the submitted application for completion.
     * Kill application if time expires.
     *
     * @param appId Application Id of application to be monitored
     * @return true if application completed successfully
     * @throws YarnException
     * @throws IOException
     */
    private boolean monitorApplication(ApplicationId appId)
            throws YarnException, IOException {

        while (true) {
            // Check app status every 1 second.
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                LOG.debug("Thread sleep in monitoring loop interrupted");
            }

            // Get application report for the appId we are interested in
            ApplicationReport report = yarnClient.getApplicationReport(appId);

            LOG.info("Got application report from ASM for"
                    + ", appId=" + appId.getId()
                    + ", clientToAMToken=" + report.getClientToAMToken()
                    + ", appDiagnostics=" + report.getDiagnostics()
                    + ", appMasterHost=" + report.getHost()
                    + ", appQueue=" + report.getQueue()
                    + ", appMasterRpcPort=" + report.getRpcPort()
                    + ", appStartTime=" + report.getStartTime()
                    + ", yarnAppState=" + report.getYarnApplicationState().toString()
                    + ", distributedFinalState=" + report.getFinalApplicationStatus().toString()
                    + ", appTrackingUrl=" + report.getTrackingUrl()
                    + ", appUser=" + report.getUser());

            YarnApplicationState state = report.getYarnApplicationState();
            FinalApplicationStatus dsStatus = report.getFinalApplicationStatus();
            if (YarnApplicationState.FINISHED == state) {
                if (FinalApplicationStatus.SUCCEEDED == dsStatus) {
                    LOG.info("Application has completed successfully. Breaking monitoring loop");
                    return true;
                } else {
                    LOG.info("Application did finished unsuccessfully."
                            + " YarnState=" + state.toString() + ", DSFinalStatus=" + dsStatus.toString()
                            + ". Breaking monitoring loop");
                    return false;
                }
            } else if (YarnApplicationState.KILLED == state
                    || YarnApplicationState.FAILED == state) {
                LOG.info("Application did not finish."
                        + " YarnState=" + state.toString() + ", DSFinalStatus=" + dsStatus.toString()
                        + ". Breaking monitoring loop");
                return false;
            } else if (YarnApplicationState.RUNNING == state) {
                if (autoexit) {
                    LOG.info("Auto exit while application has been started successfully.");
                    return true;
                }
            }
        }
    }

    /**
     * Kill a submitted application by sending a call to the ASM
     *
     * @param appId Application Id to be killed.
     * @throws YarnException
     * @throws IOException
     */
    private void forceKillApplication(ApplicationId appId)
            throws YarnException, IOException {
        // Response can be ignored as it is non-null on success or
        // throws an exception in case of failures
        yarnClient.killApplication(appId);
    }

    private void addToLocalResources(FileSystem fs, String fileSrcPath,
                                     String fileDstPath, String appId, Map<String, LocalResource> localResources,
                                     String resources) throws IOException {
        String suffix =
                appName + "/" + appId + "/" + fileDstPath;
        Path dst =
                new Path(fs.getHomeDirectory(), suffix);
        if (fileSrcPath == null) {
            FSDataOutputStream ostream = null;
            try {
                ostream = FileSystem
                        .create(fs, dst, new FsPermission((short) 0710));
                ostream.writeUTF(resources);
            } finally {
                IOUtils.closeQuietly(ostream);
            }
        } else {
            fs.copyFromLocalFile(new Path(fileSrcPath), dst);
        }
        FileStatus scFileStatus = fs.getFileStatus(dst);
        LocalResource scRsrc =
                LocalResource.newInstance(
                        ConverterUtils.getYarnUrlFromURI(dst.toUri()),
                        LocalResourceType.FILE, LocalResourceVisibility.APPLICATION,
                        scFileStatus.getLen(), scFileStatus.getModificationTime());
        localResources.put(fileDstPath, scRsrc);
    }
}