package com.neoremind.app.on.yarn.demo;

import com.google.common.base.Throwables;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.http.HtmlQuoting;
import org.apache.hadoop.util.ReflectionUtils;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.Closeable;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.BindException;

public class NestoYarnHttpServer implements Closeable {

    private static final Log LOG = LogFactory.getLog(NestoYarnHttpServer.class);

    private Server server;

    private String name;

    public void start(String name, int port) {
        this.name = name;
        int retryCount = 10;
        for (int i = 0; i < retryCount; i++) {
            try {
                server = new Server(port);
                Context context = new Context();
                context.setContextPath("/");
                context.addServlet(new ServletHolder(new WelcomeServlet(name, System.currentTimeMillis())),
                        "/");
                context.addServlet(StackServlet.class, "/stack");
                server.setHandler(context);
                server.start();
                LOG.info("Embedded Jetty has successfully started on port " + port);
                break;
            } catch (BindException e) {
                LOG.warn("Jetty server port conflicts on " + port);
                port++;
            } catch (Exception e) {
                Throwables.propagate(e);
            }
        }
    }

    public int getHttpPort() {
        return server.getConnectors()[0].getLocalPort();
    }

    public String getName() {
        return name;
    }

    public static void main(String[] args) throws InterruptedException {
        NestoYarnHttpServer server = new NestoYarnHttpServer();
        server.start("Test server", 8390);
    }

    @Override
    public void close() throws IOException {
        try {
            server.stop();
        } catch (Exception e) {
            LOG.error("Failed to shut down server due to " + e.getMessage(), e);
        }
    }

    public static class WelcomeServlet extends HttpServlet {

        private String name;

        private long startTimeInMs;

        public WelcomeServlet(String name, long startTimeInMs) {
            this.name = name;
            this.startTimeInMs = startTimeInMs;
        }

        @Override
        public void doGet(HttpServletRequest request, HttpServletResponse response)
                throws ServletException, IOException {
            response.setContentType("text/html; charset=utf-8");
            response.setStatus(HttpServletResponse.SC_OK);
            try (PrintWriter out = response.getWriter()) {
                out.println("<h2>" + name + "</h2>");
                out.println(String.format("The server has started for %d secs",
                        (System.currentTimeMillis() - startTimeInMs) / 1000));
            }
        }
    }

    public static class StackServlet extends HttpServlet {
        @Override
        public void doGet(HttpServletRequest request, HttpServletResponse response)
                throws ServletException, IOException {
            try (PrintWriter out = new PrintWriter
                    (HtmlQuoting.quoteOutputStream(response.getOutputStream()))) {
                ReflectionUtils.printThreadInfo(out, "");
                out.close();
            }
            ReflectionUtils.logThreadInfo(LOG, "jsp requested", 1);
        }
    }
}