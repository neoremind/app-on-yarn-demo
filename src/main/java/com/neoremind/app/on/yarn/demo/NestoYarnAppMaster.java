package com.neoremind.app.on.yarn.demo;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.log4j.LogManager;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

// Modified from DistributedShell ApplicationMaster in YARN Example
public class NestoYarnAppMaster {
    private static final Log LOG = LogFactory.getLog(NestoYarnAppMaster.class);

    // Configuration
    private Configuration conf;

    // Handle to communicate with the Resource Manager
    @SuppressWarnings("rawtypes")
    private AMRMClientAsync amRMClient;

    // In both secure and non-secure modes, this points to the job-submitter.
    private UserGroupInformation appSubmitterUgi;

    // Handle to communicate with the Node Manager
    private NMClientAsync nmClientAsync;
    // Listen to process the response from the Node Manager
    private NMCallbackHandler containerListener;

    // Application Attempt Id ( combination of attemptId and fail count )
    @VisibleForTesting
    protected ApplicationAttemptId appAttemptID;

    // TODO For status update for clients - yet to be implemented
    // Hostname of the container
    private String appMasterHostname = "";
    // Port on which the app master listens for status updates from clients
    private int appMasterRpcPort = -1;
    // Tracking url to which app master publishes info for clients to monitor
    private String appMasterTrackingUrl = "";

    // App Master configuration
    // No. of containers to run shell command on
    @VisibleForTesting
    protected int numTotalContainers = 1;
    // Memory to request for the container on which the shell command will run
    private int containerMemory = 1;
    // VirtualCores to request for the container on which the shell command will run
    private int containerVirtualCores = 1;

    private String coordOpts = null;

    // Counter for completed containers ( complete denotes successful or failed )
    private AtomicInteger numCompletedContainers = new AtomicInteger();
    // Allocated container count so that we know how many containers has the RM
    // allocated to us
    @VisibleForTesting
    protected AtomicInteger numAllocatedContainers = new AtomicInteger();
    // Count of failed containers
    private AtomicInteger numFailedContainers = new AtomicInteger();

    // Args to be passed to the shell command
    private String shellArgs = "";
    // Env variables to be setup for the shell command
    private Map<String, String> shellEnv = new HashMap<String, String>();

    // Hardcoded path to custom log_properties
    private static final String log4jPath = "log4j.properties";

    private static final String shellArgsPath = "shellArgs";

    private boolean enableJMX = false;

    private volatile boolean done;

    private ByteBuffer allTokens;

    // Launch threads
    private List<Thread> launchThreads = new ArrayList<Thread>();

    // All lived nodes in cluster
    private Map<NodeId, NodeReport> livedNodes = new ConcurrentHashMap<>();

    // Already used node
    private Map<NodeId, Container> usedNodes = new ConcurrentHashMap<>();

    private Set<Container> deadNodes = Sets.newSetFromMap(new ConcurrentHashMap<Container, Boolean>());

    private Set<NodeId> blackList = null;

    private ConcurrentHashMap<ContainerId, Container> runningContainers = new ConcurrentHashMap<>();

    // Count of containers already requested from the RM
    // Needed as once requested, we should not request for containers again.
    // Only request for more if the original requirement changes.
    @VisibleForTesting
    private ContainerRequestsWithTimeout requests;

    private YarnClient yarnClient;

    private NestoYarnHttpServer httpServer;

    // Debug flag
    boolean debugFlag = false;

    // in MB
    private int memoryOverhead = 384;

    /**
     * @param args Command line args
     */
    public static void main(String[] args) {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));

        boolean result = false;
        try {
            NestoYarnAppMaster appMaster = new NestoYarnAppMaster();
            LOG.info("Initializing ApplicationMaster:" + Arrays.toString(args));
            boolean doRun = appMaster.init(args);
            if (!doRun) {
                System.exit(0);
            }
            appMaster.run();
            result = appMaster.finish();
        } catch (Throwable t) {
            LOG.fatal("Error running ApplicationMaster", t);
            LogManager.shutdown();
            ExitUtil.terminate(1, t);
        }
        if (result) {
            LOG.info("Application Master completed successfully. exiting");
            System.exit(0);
        } else {
            LOG.info("Application Master failed. exiting");
            System.exit(2);
        }
    }

    public NestoYarnAppMaster() {
        // Set up the configuration
        conf = new YarnConfiguration();
    }

    /**
     * Parse command line options
     *
     * @param args Command line args
     * @return Whether init successful and run should be invoked
     * @throws ParseException
     * @throws IOException
     */
    public boolean init(String[] args) throws ParseException, IOException {
        Options opts = new Options();
        opts.addOption("app_attempt_id", true,
                "App Attempt ID. Not to be used unless for testing purposes");
        opts.addOption("shell_env", true,
                "Environment for shell script. Specified as env_key=env_val pairs");
        opts.addOption("container_memory", true,
                "Amount of memory in MB to be requested to run the shell command");
        opts.addOption("container_vcores", true,
                "Amount of virtual cores to be requested to run the shell command");
        opts.addOption("num_containers", true,
                "No. of containers on which the shell command needs to be executed");
        opts.addOption("coord_memory", true, "Amount of memory in MB to be requested to run nesto coord in container");
        opts.addOption("num_coords", true, "No. of containers on which run the nesto coord");
        opts.addOption("coord_opts", true, "Java opts for starting coord");
        opts.addOption("memory_overhead", true,
                "Amount of memory overhead in MB for container");
        opts.addOption("enable_jmx", false, "Enable JMX");
        opts.addOption("debug", false, "Dump out debug information");
        opts.addOption("node_label_expression", true,
                "Node label expression to determine the nodes"
                        + " where all the containers of this application"
                        + " will be allocated, \"\" means containers"
                        + " can be allocated anywhere, if you don't specify the option,"
                        + " default node_label_expression of queue will be used.");
        opts.addOption("help", false, "Print usage");
        CommandLine cliParser = new GnuParser().parse(opts, args);

        if (args.length == 0) {
            printUsage(opts);
            throw new IllegalArgumentException(
                    "No args specified for application master to initialize");
        }

        //Check whether customer log4j.properties file exists
        if (NestoYarnHelper.fileExist(log4jPath)) {
            try {
                Log4jPropertyHelper.updateLog4jConfiguration(NestoYarnAppMaster.class,
                        log4jPath);
            } catch (Exception e) {
                LOG.warn("Can not set up custom log4j properties. " + e);
            }
        }

        if (cliParser.hasOption("help")) {
            printUsage(opts);
            return false;
        }

        if (cliParser.hasOption("debug")) {
            NestoYarnHelper.dumpOutDebugInfo(LOG);
            debugFlag = true;
        }

        if (cliParser.hasOption("enable_jmx")) {
            enableJMX = true;
        }

        Map<String, String> envs = System.getenv();

        if (!envs.containsKey(ApplicationConstants.Environment.CONTAINER_ID.name())) {
            if (cliParser.hasOption("app_attempt_id")) {
                String appIdStr = cliParser.getOptionValue("app_attempt_id", "");
                appAttemptID = ConverterUtils.toApplicationAttemptId(appIdStr);
            } else {
                throw new IllegalArgumentException(
                        "Application Attempt Id not set in the environment");
            }
        } else {
            ContainerId containerId = ConverterUtils.toContainerId(envs
                    .get(ApplicationConstants.Environment.CONTAINER_ID.name()));
            appAttemptID = containerId.getApplicationAttemptId();
        }

        if (!envs.containsKey(ApplicationConstants.APP_SUBMIT_TIME_ENV)) {
            throw new RuntimeException(ApplicationConstants.APP_SUBMIT_TIME_ENV
                    + " not set in the environment");
        }
        if (!envs.containsKey(ApplicationConstants.Environment.NM_HOST.name())) {
            throw new RuntimeException(ApplicationConstants.Environment.NM_HOST.name()
                    + " not set in the environment");
        }
        if (!envs.containsKey(ApplicationConstants.Environment.NM_HTTP_PORT.name())) {
            throw new RuntimeException(ApplicationConstants.Environment.NM_HTTP_PORT
                    + " not set in the environment");
        }
        if (!envs.containsKey(ApplicationConstants.Environment.NM_PORT.name())) {
            throw new RuntimeException(ApplicationConstants.Environment.NM_PORT.name()
                    + " not set in the environment");
        }

        LOG.info("Application master for app" + ", appId="
                + appAttemptID.getApplicationId().getId() + ", clustertimestamp="
                + appAttemptID.getApplicationId().getClusterTimestamp()
                + ", attemptId=" + appAttemptID.getAttemptId());

        if (cliParser.hasOption("shell_env")) {
            String shellEnvs[] = cliParser.getOptionValues("shell_env");
            for (String env : shellEnvs) {
                env = env.trim();
                int index = env.indexOf('=');
                if (index == -1) {
                    shellEnv.put(env, "");
                    continue;
                }
                String key = env.substring(0, index);
                String val = "";
                if (index < (env.length() - 1)) {
                    val = env.substring(index + 1);
                }
                shellEnv.put(key, val);
            }
        }

        containerMemory = Integer.parseInt(cliParser.getOptionValue(
                "container_memory", "10"));
        containerVirtualCores = Integer.parseInt(cliParser.getOptionValue(
                "container_vcores", "1"));
        numTotalContainers = Integer.parseInt(cliParser.getOptionValue(
                "num_containers", "1"));
        coordOpts = cliParser.getOptionValue("coord_opts");

        if (!cliParser.hasOption("memory_overhead")) {
            memoryOverhead = Math.max((int) (containerMemory * 0.1), 384);
        } else
            memoryOverhead = Integer.parseInt(cliParser.getOptionValue(
                    "memory_overhead", "384"));

        if (numTotalContainers == 0) {
            throw new IllegalArgumentException(
                    "Cannot run distributed shell with no containers");
        }

        return true;
    }

    /**
     * Helper function to print usage
     *
     * @param opts Parsed command line options
     */
    private void printUsage(Options opts) {
        new HelpFormatter().printHelp("ApplicationMaster", opts);
    }

    /**
     * Main run function for the application master
     *
     * @throws YarnException
     * @throws IOException
     */
    @SuppressWarnings({"unchecked"})
    public void run() throws YarnException, IOException {
        LOG.info("Starting ApplicationMaster");

        blackList = Collections.newSetFromMap(new LinkedHashMap<NodeId, Boolean>() {
            protected boolean removeEldestEntry(Map.Entry<NodeId, Boolean> eldest) {
                if (livedNodes == null)
                    return false;
                return livedNodes.size() * 0.8 < (usedNodes.size() + size());
            }
        });

        // Note: Credentials, Token, UserGroupInformation, DataOutputBuffer class
        // are marked as LimitedPrivate
        Credentials credentials =
                UserGroupInformation.getCurrentUser().getCredentials();
        DataOutputBuffer dob = new DataOutputBuffer();
        credentials.writeTokenStorageToStream(dob);
        // Now remove the AM->RM token so that containers cannot access it.
        Iterator<Token<?>> iter = credentials.getAllTokens().iterator();
        LOG.info("Executing with tokens:");
        while (iter.hasNext()) {
            Token<?> token = iter.next();
            LOG.info(token);
            if (token.getKind().equals(AMRMTokenIdentifier.KIND_NAME)) {
                iter.remove();
            }
        }
        allTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());

        // Create appSubmitterUgi and add original tokens to it
        String appSubmitterUserName =
                System.getenv(ApplicationConstants.Environment.USER.name());
        appSubmitterUgi =
                UserGroupInformation.createRemoteUser(appSubmitterUserName);
        appSubmitterUgi.addCredentials(credentials);

        AMRMClientAsync.CallbackHandler allocListener = new RMCallbackHandler(this);
        amRMClient = AMRMClientAsync.createAMRMClientAsync(1000, allocListener);
        amRMClient.init(conf);
        amRMClient.start();

        requests = new ContainerRequestsWithTimeout();

        containerListener = createNMCallbackHandler();
        nmClientAsync = new NMClientAsyncImpl(containerListener);
        nmClientAsync.init(conf);
        nmClientAsync.start();

        yarnClient = YarnClient.createYarnClient();
        yarnClient.init(conf);
        yarnClient.start();
        setupUpdateLivedNodesThread();

        httpServer = new NestoYarnHttpServer();
        httpServer.start(8090);

        // Register self with ResourceManager
        // This will start heartbeating to the RM
        appMasterHostname = NetUtils.getHostname();
        appMasterTrackingUrl = "http://" + NetworkUtils.getLocalHostIP() + ":"
                + httpServer.getHttpPort();
        LOG.info("ApplicationMaster tracking url:" + appMasterTrackingUrl);

        RegisterApplicationMasterResponse response = amRMClient
                .registerApplicationMaster(appMasterHostname, appMasterRpcPort,
                        appMasterTrackingUrl);
        // Dump out information about cluster capability as seen by the
        // resource manager
        int maxMem = response.getMaximumResourceCapability().getMemory();
        LOG.info("Max mem capabililty of resources in this cluster " + maxMem);

        int maxVCores = response.getMaximumResourceCapability().getVirtualCores();
        LOG.info("Max vcores capabililty of resources in this cluster " + maxVCores);

        // A resource ask cannot exceed the max.
        if (containerMemory + memoryOverhead > maxMem) {
            LOG.info("Container memory specified above max threshold of cluster."
                    + " Using max value." + ", specified=" + (containerMemory + memoryOverhead) + ", max="
                    + maxMem);
            containerMemory = maxMem - memoryOverhead;
        }

        if (containerVirtualCores > maxVCores) {
            LOG.info("Container virtual cores specified above max threshold of cluster."
                    + " Using max value." + ", specified=" + containerVirtualCores + ", max="
                    + maxVCores);
            containerVirtualCores = maxVCores;
        }

        List<Container> previousAMRunningContainers =
                response.getContainersFromPreviousAttempts();
        LOG.info(appAttemptID + " received " + previousAMRunningContainers.size()
                + " previous attempts' running containers on AM registration.");
        numAllocatedContainers.addAndGet(previousAMRunningContainers.size());

        recoverExecutors(previousAMRunningContainers);

        int numTotalContainersToRequest =
                numTotalContainers - previousAMRunningContainers.size();
        // Setup ask for containers from RM
        // Send request for containers to RM
        // Until we get our fully allocated quota, we keep on polling RM for
        // containers
        // Keep looping until all the containers are launched and shell script
        // executed on them ( regardless of success/failure).
        requestKContainersFromKNodes(numTotalContainersToRequest);
    }

    private void recoverExecutors(List<Container> previousAMRunningContainers) {
        for (Container container : previousAMRunningContainers) {
            usedNodes.put(container.getNodeId(), container);
            runningContainers.putIfAbsent(container.getId(), container);
        }
    }

    @VisibleForTesting
    NMCallbackHandler createNMCallbackHandler() {
        return new NMCallbackHandler(this);
    }

    @VisibleForTesting
    protected boolean finish() {
        // wait for completion.
        while (!done) {
            try {
                Thread.sleep(200);
            } catch (InterruptedException ex) {
            }
        }

        // Join all launched threads
        // needed for when we time out
        // and we need to release containers
        for (Thread launchThread : launchThreads) {
            try {
                launchThread.join(10000);
            } catch (InterruptedException e) {
                LOG.info("Exception thrown in thread join: " + e.getMessage());
                e.printStackTrace();
            }
        }

        // When the application completes, it should stop all running containers
        LOG.info("Application completed. Stopping running containers");
        nmClientAsync.stop();

        // When the application completes, it should send a finish application
        // signal to the RM
        LOG.info("Application completed. Signalling finish to RM");

        FinalApplicationStatus appStatus;
        String appMessage = null;
        boolean success = true;
        if (numFailedContainers.get() == 0 &&
                numCompletedContainers.get() == numTotalContainers) {
            appStatus = FinalApplicationStatus.SUCCEEDED;
        } else {
            appStatus = FinalApplicationStatus.FAILED;
            appMessage = "Diagnostics." + ", total=" + numTotalContainers
                    + ", completed=" + numCompletedContainers.get() + ", allocated="
                    + numAllocatedContainers.get() + ", failed="
                    + numFailedContainers.get();
            LOG.info(appMessage);
            success = false;
        }
        try {
            amRMClient.unregisterApplicationMaster(appStatus, appMessage, null);
        } catch (YarnException ex) {
            LOG.error("Failed to unregister application", ex);
        } catch (IOException e) {
            LOG.error("Failed to unregister application", e);
        }

        amRMClient.stop();

        yarnClient.stop();

        return success;
    }

    public void killContainer(Container container) {
        amRMClient.releaseAssignedContainer(container.getId());
    }

    public static <T extends Comparable<? super T>> List<T> asSortedList(Collection<T> c, Comparator<T> cmp) {
        List<T> list = new ArrayList<T>(c);
        Collections.sort(list, cmp);
        return list;
    }

    public ConcurrentHashMap<ContainerId, Container> getRunningContainers() {
        return runningContainers;
    }

    public Resource getNodeAvailableResource(NodeReport nodeReport) {
        Resource c = nodeReport.getCapability();
        Resource u = nodeReport.getUsed();
        return Resource.newInstance(c.getMemory() - u.getMemory(), c.getVirtualCores() - u.getVirtualCores());
    }


    private void requestKContainersFromKNodes(int askCount) {
        int counter = 0;
        Set<NodeId> unusedNodes = Sets.difference(livedNodes.keySet(), usedNodes.keySet());
        if (unusedNodes.size() == 0) {
            LOG.info("No used nodes, stop.");
            return;
        }
        List<NodeId> prorityList = asSortedList(unusedNodes, new Comparator<NodeId>() {
            @Override
            public int compare(NodeId o1, NodeId o2) {
                NodeReport nodeReport = livedNodes.get(o1);
                NodeReport nodeReport2 = livedNodes.get(o2);
                if (nodeReport == nodeReport2) return 0;
                if (nodeReport == null) return 1;
                if (nodeReport2 == null) return -1;

                Resource r1 = getNodeAvailableResource(nodeReport);
                Resource r2 = getNodeAvailableResource(nodeReport2);
                if (r1.getMemory() == r2.getMemory()) return r2.getVirtualCores() - r1.getVirtualCores();
                if (r1.getVirtualCores() == r2.getVirtualCores()) return r2.getMemory() - r1.getMemory();
                if (r1.getMemory() >= r2.getMemory() && r1.getVirtualCores() >= r2.getVirtualCores()) return -1;
                if (r1.getMemory() <= r2.getMemory() && r1.getVirtualCores() <= r2.getVirtualCores()) return 1;
                if (r1.getMemory() >= containerMemory && r2.getMemory() >= containerMemory)
                    return r2.getVirtualCores() - r1.getVirtualCores();
                return r2.getMemory() - r1.getMemory();
            }
        });

        for (NodeId nodeId : prorityList) {
            if (counter >= askCount) break;
            if (requests.isNodePending(nodeId)) {
                LOG.info("Node pending for " + nodeId);
                continue;
            }
            Boolean isBlackList = blackList.contains(nodeId);
            if (isBlackList) {
                LOG.info("Node: " + nodeId + " in blacklist: " + blackList);
                continue;
            }
            NodeReport nodeReport = livedNodes.get(nodeId);
            if (nodeReport == null || usedNodes.containsKey(nodeReport.getNodeId())) continue;

            Resource resource = getNodeAvailableResource(nodeReport);
            if (counter < 1)
                LOG.info("Node " + nodeReport.getNodeId() + nodeReport.getRackName() + " resources: " + resource);
            if (counter > 4 && (resource.getMemory() < containerMemory || resource.getVirtualCores() < containerVirtualCores)) {
                LOG.debug("Ignore node with limited available resource: " + resource);
                continue;
            }
            AMRMClient.ContainerRequest containerAsk = setupContainerAskForRM(
                    nodeReport.getNodeId().getHost(), nodeReport.getRackName(), containerMemory + memoryOverhead);
            LOG.info("#" + counter + " Requested container ask: " + containerAsk.toString()
                    + " from container: " + nodeReport.getNodeId() + nodeReport.getRackName() + " resources: " + resource
                    + "locality: " + containerAsk.getRelaxLocality());
            requests.addRequest(nodeReport.getNodeId(), containerAsk);
            counter++;
        }

        LOG.info("Request new Containers:" + askCount + ", usedNodes:" + usedNodes.size()
                + ", so far numRequestedContainers:" + requests.getPendingRequestCount() + " liveNodes:" + livedNodes.size()
                + " unused nodes: " + unusedNodes.size() + " counter: " + counter + " blackList:" + blackList.size());
        if (counter == 0)
            blackList.clear();
    }

    private synchronized void askMoreContainersIfNeccessary() {
        int askCount = numTotalContainers - requests.getPendingRequestCount() - usedNodes.size();
        if (askCount > 0) {
            LOG.info("Request More Containers:" + askCount
                    + ", so far numRequestedContainers:" + requests.getPendingRequestCount());
            requestKContainersFromKNodes(askCount);
        }
    }

    public Collection<Container> getUsedNodes() {
        return usedNodes.values();
    }

    public Collection<Container> getDeadNodes() {
        return deadNodes;
    }

    private class RMCallbackHandler implements AMRMClientAsync.CallbackHandler {
        private Set<ContainerId> containersToRelease = new HashSet<>();
        private final NestoYarnAppMaster applicationMaster;

        public RMCallbackHandler(NestoYarnAppMaster applicationMaster) {
            this.applicationMaster = applicationMaster;
        }

        @Override
        public void onContainersCompleted(List<ContainerStatus> completedContainers) {
            LOG.info("Got response from RM for container ask, completedCnt="
                    + completedContainers.size());
            for (ContainerStatus containerStatus : completedContainers) {
                LOG.info(appAttemptID + " got container status for containerID="
                        + containerStatus.getContainerId() + ", state="
                        + containerStatus.getState() + ", exitStatus="
                        + containerStatus.getExitStatus() + ", diagnostics="
                        + containerStatus.getDiagnostics());

                // non complete containers should not be here
                assert (containerStatus.getState() == ContainerState.COMPLETE);

                // increment counters for completed/failed containers
                int exitStatus = containerStatus.getExitStatus();
                if (0 != exitStatus) {
                    // container was killed by framework, possibly preempted
                    // we should re-try as the container was lost for some reason
                    numAllocatedContainers.decrementAndGet();
                    if (containersToRelease.contains(containerStatus.getContainerId())) {
                        LOG.info("Finally Container:" + containerStatus.getContainerId() + " is released.");
                        containersToRelease.remove(containerStatus.getContainerId());
                        continue;
                    }
                    Container container = runningContainers.remove(containerStatus.getContainerId());
                    if (container != null) {
                        applicationMaster.numFailedContainers.incrementAndGet();
                        deadNodes.add(container);
                        usedNodes.remove(container.getNodeId());
                        LOG.info("Killed/Failed Container:" + containerStatus.getContainerId()
                                + " is removed from runningContainers.");
                        if (debugFlag && numFailedContainers.get() > 3) {
                            LOG.error("In debug mode, number of failed containers exceeds 3. Killing myself......");
                            System.exit(1);
                        }
                    }
                } else {
                    // nothing to do
                    // container completed successfully
                    numCompletedContainers.incrementAndGet();
                    LOG.info("Container completed successfully." + ", containerId="
                            + containerStatus.getContainerId());
                }
            }

            // ask for more containers if any failed
            askMoreContainersIfNeccessary();
        }

        @Override
        public void onContainersAllocated(List<Container> allocatedContainers) {
            LOG.info("Got response from RM for container ask, allocatedCnt="
                    + allocatedContainers.size());
            numAllocatedContainers.addAndGet(allocatedContainers.size());
            for (Container allocatedContainer : allocatedContainers) {
                requests.gotContainer(allocatedContainer);
                if (usedNodes.containsKey(allocatedContainer.getNodeId()) ||
                        usedNodes.size() >= numTotalContainers) {
                    if (usedNodes.containsKey(allocatedContainer.getNodeId())) {
                        LOG.info("Release an container on already used node:" + allocatedContainer.getNodeId());
                    } else {
                        LOG.info("Number of Containers has reached " + numTotalContainers
                                + ", release container from:" + allocatedContainer.getNodeId());
                    }
                    amRMClient.releaseAssignedContainer(allocatedContainer.getId());
                    containersToRelease.add(allocatedContainer.getId());
                    continue;
                }
                usedNodes.put(allocatedContainer.getNodeId(), allocatedContainer);
                LOG.info("Launching shell command on a new container."
                        + ", containerId=" + allocatedContainer.getId()
                        + ", containerNode=" + allocatedContainer.getNodeId().getHost()
                        + ":" + allocatedContainer.getNodeId().getPort()
                        + ", containerNodeURI=" + allocatedContainer.getNodeHttpAddress()
                        + ", containerResourceMemory"
                        + allocatedContainer.getResource().getMemory()
                        + ", containerResourceVirtualCores"
                        + allocatedContainer.getResource().getVirtualCores());

                LaunchContainerRunnable runnableLaunchContainer =
                        new LaunchContainerRunnable(allocatedContainer, containerListener);
                Thread launchThread = new Thread(runnableLaunchContainer);

                // launch and start the container on a separate thread to keep
                // the main thread unblocked
                // as all containers may not be allocated at one go.
                launchThreads.add(launchThread);
                launchThread.start();
            }
        }

        @Override
        public void onShutdownRequest() {
            LOG.info("Got ShutdownRequest");
            done = true;
        }

        @Override
        public void onNodesUpdated(List<NodeReport> updatedNodes) {
        }

        @Override
        public float getProgress() {
            // set progress to deliver to RM on next heartbeat
            float progress = (float) numCompletedContainers.get()
                    / numTotalContainers;
            return progress;
        }

        @Override
        public void onError(Throwable e) {
            LOG.info("Got Error", e);
            done = true;
            amRMClient.stop();
        }
    }

    @VisibleForTesting
    class NMCallbackHandler
            implements NMClientAsync.CallbackHandler {
        private final NestoYarnAppMaster applicationMaster;

        public NMCallbackHandler(NestoYarnAppMaster applicationMaster) {
            this.applicationMaster = applicationMaster;
        }

        public void addContainer(ContainerId containerId, Container container) {
            runningContainers.putIfAbsent(containerId, container);
        }

        @Override
        public void onContainerStopped(ContainerId containerId) {
            LOG.info("Succeeded to stop Container " + containerId);
            Container container = runningContainers.remove(containerId);
            if (container != null)
                usedNodes.remove(container.getNodeId());
        }

        @Override
        public void onContainerStatusReceived(ContainerId containerId,
                                              ContainerStatus containerStatus) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Container Status: id=" + containerId + ", status=" +
                        containerStatus);
            }
        }

        @Override
        public void onContainerStarted(ContainerId containerId,
                                       Map<String, ByteBuffer> allServiceResponse) {
            LOG.info("Succeeded to start Container " + containerId);
            Container container = runningContainers.get(containerId);
            if (container != null) {
                applicationMaster.nmClientAsync.getContainerStatusAsync(containerId, container.getNodeId());
            }
        }

        @Override
        public void onStartContainerError(ContainerId containerId, Throwable t) {
            LOG.error("Failed to start Container " + containerId);
            Container container = runningContainers.remove(containerId);
            if (container != null)
                usedNodes.remove(container.getNodeId());
            applicationMaster.numCompletedContainers.incrementAndGet();
            applicationMaster.numFailedContainers.incrementAndGet();
        }

        @Override
        public void onGetContainerStatusError(
                ContainerId containerId, Throwable t) {
            LOG.error("Failed to query the status of Container " + containerId);
        }

        @Override
        public void onStopContainerError(ContainerId containerId, Throwable t) {
            LOG.error("Failed to stop Container " + containerId);
            Container container = runningContainers.remove(containerId);
            if (container != null)
                usedNodes.remove(container.getNodeId());
        }
    }

    /**
     * Thread to connect to the {@link ContainerManagementProtocol} and launch the container
     * that will execute the shell command.
     */
    private class LaunchContainerRunnable implements Runnable {

        // Allocated container
        Container container;

        NMCallbackHandler containerListener;

        /**
         * @param lcontainer        Allocated container
         * @param containerListener Callback handler of the container
         */
        public LaunchContainerRunnable(
                Container lcontainer, NMCallbackHandler containerListener) {
            this.container = lcontainer;
            this.containerListener = containerListener;
        }

        @Override
        /**
         * Connects to CM, sets up container launch context
         * for shell command and eventually dispatches the container
         * start request to the CM.
         */
        public void run() {
            int mem = containerMemory;

            LOG.info("Setting up container launch container for containerid=" + container.getId()
                    + " " + container.getResource());

            Map<String, String> currentEnvs = System.getenv();
            if (!currentEnvs.containsKey(NestoYarnConstants.NESTO_YARN_FRAMEWORK_PATH)) {
                throw new RuntimeException(NestoYarnConstants.NESTO_YARN_FRAMEWORK_PATH
                        + " not set in the environment.");
            }
            String frameworkPath = currentEnvs.get(NestoYarnConstants.NESTO_YARN_FRAMEWORK_PATH);

            shellEnv.put("CLASSPATH", NestoYarnHelper.buildClassPathEnv(conf));

            // Set the local resources
            Map<String, LocalResource> localResources = new HashMap<>();

            try {
                FileSystem fs = FileSystem.get(conf);
                NestoYarnHelper.addFrameworkToDistributedCache(frameworkPath, localResources, conf);
            } catch (IOException e) {
                e.printStackTrace();
            }

            // Set the necessary command to execute on the allocated container
            Vector<CharSequence> vargs = new Vector<CharSequence>(10);

            // Set java executable command
            LOG.info("Setting up app command on " + container.getNodeId().getHost());
            try {
                String log4jContents = Files.toString(new File(NestoYarnHelper.getAppContainerLog4jFile()),
                        Charsets.UTF_8);
                vargs.add("echo '" + log4jContents + "' >$PWD/" + NestoYarnConstants.NESTO_YARN_APPCONTAINER_LOG4J);
                vargs.add(" && ");
            } catch (Exception e) {
                throw new RuntimeException("log4j file not existed:" + NestoYarnHelper.getAppContainerLog4jFile());
            }

            vargs.add(ApplicationConstants.Environment.JAVA_HOME.$$() + "/bin/java");
            // Set am memory size
            vargs.add("-Xms" + mem + "m");
            vargs.add("-Xmx" + mem + "m");
            vargs.add("-Djava.io.tmpdir=$PWD/tmp");
            vargs.add("-Dlog4j.configuration=" + NestoYarnConstants.NESTO_YARN_APPCONTAINER_LOG4J);

            if (enableJMX) {
                vargs.add(NestoYarnHelper.buildJmxJvmProperty());
            }

            // Set class name
            vargs.add(NestoYarnConstants.NESTO_YARN_SERVER_MAINCLASS);

            // Set args for the shell command if any
            vargs.add(shellArgs);
            // Add log redirect params
            vargs.add("1>>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/"
                    + NestoYarnConstants.NESTO_YARN_APPCONTAINER_LOG);
            vargs.add("2>>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/"
                    + NestoYarnConstants.NESTO_YARN_APPCONTAINER_LOG);

            // Get final command
            StringBuilder command = new StringBuilder();
            for (CharSequence str : vargs) {
                command.append(str).append(" ");
            }

            LOG.info("Launch container: " + command.toString());
            List<String> commands = new ArrayList<String>();
            commands.add(command.toString());

            // Set up ContainerLaunchContext, setting local resource, environment,
            // command and token for constructor.
            ContainerLaunchContext ctx = ContainerLaunchContext.newInstance(
                    localResources, shellEnv, commands, null, allTokens.duplicate(), null);
            containerListener.addContainer(container.getId(), container);
            nmClientAsync.startContainerAsync(container, ctx);
        }
    }

    /**
     * Setup the request that will be sent to the RM for the container ask.
     *
     * @return the setup ResourceRequest to be sent to RM
     */
    private AMRMClient.ContainerRequest setupContainerAskForRM(String hostname, String rackname, int mem) {
        // setup requirements for hosts
        // using * as any host will do for the distributed shell app
        // set the priority for the request
        Priority pri = Priority.newInstance(NestoYarnConstants.NESTO_YARN_SERVER_PRIORITY);

        // Set up resource type requirements
        // For now, memory and CPU are supported so we set memory and cpu requirements
        Resource capability = Resource.newInstance(mem, containerVirtualCores);
        //AMRMClient.ContainerRequest request = new AMRMClient.ContainerRequest(capability, new String[]{hostname},
        //        new String[]{rackname}, pri);
        AMRMClient.ContainerRequest request = new AMRMClient.ContainerRequest(capability, null,
                null, pri);
        return request;
    }

    private void setupUpdateLivedNodesThread() {
        Executors.newScheduledThreadPool(1).scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    List<NodeReport> clusterNodeReports = yarnClient.getNodeReports(
                            NodeState.RUNNING);
                    LOG.info("Got Cluster node info from ASM, nodes number:" + clusterNodeReports.size());
                    Map<NodeId, NodeReport> nodes = new ConcurrentHashMap<>();
                    for (NodeReport nodeReport : clusterNodeReports) {
                        nodes.put(nodeReport.getNodeId(), nodeReport);
                    }
                    livedNodes = nodes;
                } catch (Exception e) {
                    LOG.error("Error when fetching live nodemanagers from resourcemanager.");
                }
            }
        }, 0, 15 * 60, TimeUnit.SECONDS);
    }

    public class ContainerRequestsWithTimeout extends TimerTask {
        Map<AMRMClient.ContainerRequest, Long> requestStartTimes;
        Map<NodeId, AMRMClient.ContainerRequest> requests;
        Set<AMRMClient.ContainerRequest> coordRequests;

        Timer timer;

        public ContainerRequestsWithTimeout() {
            this.requests = Maps.newConcurrentMap();
            this.requestStartTimes = Maps.newConcurrentMap();
            this.coordRequests = Sets.newHashSetWithExpectedSize(numTotalContainers);
            this.timer = new Timer("timer-RequestTimeout", true);
            this.timer.schedule(this, 30 * 1000, 30 * 1000);
        }

        public synchronized void addRequest(NodeId nodeId, AMRMClient.ContainerRequest request) {
            requestStartTimes.put(request, System.currentTimeMillis());
            requests.put(nodeId, request);
            try {
                amRMClient.addContainerRequest(request);
            } catch (Exception e) {
                LOG.error("Failed to send request", e);
            }
            coordRequests.add(request);
        }

        public synchronized void gotContainer(Container container) {
            NodeId nodeId = container.getNodeId();
            AMRMClient.ContainerRequest request = requests.get(nodeId);
            if (request != null) {
                requests.remove(nodeId);
                coordRequests.remove(request);
                requestStartTimes.remove(request);
            }
        }

        @Override
        public synchronized void run() {
            long threshold = System.currentTimeMillis() - 120 * 1000;
            for (Iterator<Map.Entry<NodeId, AMRMClient.ContainerRequest>> it = requests.entrySet().iterator(); it.hasNext(); ) {
                Map.Entry<NodeId, AMRMClient.ContainerRequest> entry = it.next();
                AMRMClient.ContainerRequest request = entry.getValue();
                Long starTime = requestStartTimes.get(request);
                if (starTime != null && starTime < threshold) {
                    requestTimeout(request);
                    blackList.add(entry.getKey());
                    it.remove();
                }
            }
            askMoreContainersIfNeccessary();
        }

        public void requestTimeout(AMRMClient.ContainerRequest request) {
            if (request == null)
                return;
            LOG.info("Cancel timeout request: " + request + " " + request.getNodes());
            amRMClient.removeContainerRequest(request);
            requestStartTimes.remove(request);
            coordRequests.remove(request);
        }

        boolean isNodePending(NodeId nodeId) {
            return requests.containsKey(nodeId);
        }

        public int getPendingRequestCount() {
            return requests.size();
        }

        public int getPendingCoordRequestCount() {
            return coordRequests.size();
        }

    }
}