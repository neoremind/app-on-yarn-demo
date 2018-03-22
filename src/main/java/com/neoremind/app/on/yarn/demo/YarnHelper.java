package com.neoremind.app.on.yarn.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

public class YarnHelper {

    public static String buildClassPathEnv(Configuration conf) {
        StringBuilder classPathEnv = new StringBuilder(ApplicationConstants.Environment.CLASSPATH.$$())
                .append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("$PWD/log4j.properties")
                .append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("$PWD")
                .append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("$HADOOP_CLIENT_CONF_DIR")
                .append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("$HADOOP_CONF_DIR")
                .append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("$JAVA_HOME/lib/tools.jar")
                .append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("$PWD/")
                .append(Constants.JAR_FILE_LINKEDNAME).append("/")
                .append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("$PWD/")
                .append(Constants.JAR_FILE_LINKEDNAME).append("/conf/")
                .append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("$PWD/")
                .append(Constants.JAR_FILE_LINKEDNAME).append("/*");
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
        } catch (URISyntaxException e) {
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
                    null, Constants.JAR_FILE_LINKEDNAME);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }

        FileStatus scFileStatus = fs.getFileStatus(frameworkPath);
        LocalResource scRsrc =
                LocalResource.newInstance(
                        ConverterUtils.getYarnUrlFromURI(uri),
                        LocalResourceType.ARCHIVE, LocalResourceVisibility.PRIVATE,
                        scFileStatus.getLen(), scFileStatus.getModificationTime());
        localResources.put(Constants.JAR_FILE_LINKEDNAME, scRsrc);
    }
}