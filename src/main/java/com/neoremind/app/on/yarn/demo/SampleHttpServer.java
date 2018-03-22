package com.neoremind.app.on.yarn.demo;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;

public class SampleHttpServer extends BaseHttpServer {

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
                out.println("<body>");
                out.println("<h2>" + name + "</h2>");
                out.println(String.format("<div>The server has started for %d secs</div>",
                        (System.currentTimeMillis() - startTimeInMs) / 1000));
                out.println("</body>");
            }
        }
    }

    @Override
    public HttpServlet getIndexPageServlet(String name) {
        return new WelcomeServlet(name, System.currentTimeMillis());
    }

    public static void main(String[] args) {
        SampleHttpServer httpServer = new SampleHttpServer();
        httpServer.start("test", 8290);
    }
}