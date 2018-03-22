package com.neoremind.app.on.yarn.demo;

import com.google.common.base.Throwables;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.BindException;

public class NestoYarnHttpServer {

    private static final Log LOG = LogFactory.getLog(NestoYarnHttpServer.class);

    private Server server;

    public void start(int port) {
        int retryCount = 10;
        for (int i = 0; i < retryCount; i++) {
            try {
                server = new Server(port);
                server.setHandler(new HelloHandler());
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

    public static void main(String[] args) throws InterruptedException {
        NestoYarnHttpServer server = new NestoYarnHttpServer();
        server.start(8090);
    }
}

class HelloHandler extends AbstractHandler {
    final String greeting;
    final String body;

    public HelloHandler() {
        this("Hello World");
    }

    public HelloHandler(String greeting) {
        this(greeting, null);
    }

    public HelloHandler(String greeting, String body) {
        this.greeting = greeting;
        this.body = body;
    }

    @Override
    public void handle(String s, Request request, HttpServletRequest httpServletRequest, HttpServletResponse response) throws IOException, ServletException {
        response.setContentType("text/html; charset=utf-8");
        response.setStatus(HttpServletResponse.SC_OK);

        PrintWriter out = response.getWriter();

        out.println("<h1>" + greeting + "</h1>");
        if (body != null) {
            out.println(body);
        }
    }
}
