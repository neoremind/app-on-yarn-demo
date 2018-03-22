package com.neoremind.app.on.yarn.demo;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;

import java.security.PrivilegedAction;

// An Executor to run nesto server in a yarn container
public class NestoServerExecutor {
    private static final Log LOG = LogFactory.getLog(NestoServerExecutor.class);

    public static void run(String[] args) throws Exception {
        NestoYarnHttpServer httpServer = new NestoYarnHttpServer();
        httpServer.start(8290);
    }

    public static String getCurrentUserName() throws Exception {
        // Get user from container environment
        String user = System.getenv("USER");
        if (user == null || user.isEmpty()) {
            LOG.info("Environment USER is not set, use user from UserGroupInformation.");
            user = UserGroupInformation.getCurrentUser().getShortUserName();
        }
        return user;
    }

    public static void transferCredentials(UserGroupInformation source, UserGroupInformation dest) {
        for (Token<? extends TokenIdentifier> tokenIdentifier : source.getTokens()) {
            dest.addToken(tokenIdentifier);
        }
    }

    public static void main(final String[] args) throws Exception {
        String user = getCurrentUserName();
        LOG.info("Run Server as user:" + user);
        UserGroupInformation ugi = UserGroupInformation.createRemoteUser(user);
        transferCredentials(UserGroupInformation.getCurrentUser(), ugi);
        ugi.doAs(new PrivilegedAction<Void>() {
            @Override
            public Void run() {
                try {
                    NestoServerExecutor.run(args);
                } catch (Exception e) {
                    e.printStackTrace();
                    System.exit(1);
                }
                return null;
            }
        });
    }
}
