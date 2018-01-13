package com.dropboxish.app;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.ServerProperties;
import org.glassfish.jersey.servlet.ServletContainer;

public class App {
    public static void main( String[] args ) throws Exception {

        final ResourceConfig resourceConfig = new ResourceConfig(Services.class);
        resourceConfig.packages("the_package_where_these_classes_are");
        resourceConfig.register(MultiPartFeature.class);

        ServletHolder jerseyServlet = new ServletHolder(new ServletContainer(resourceConfig));

        Server jettyServer = new Server(8080);
        ServletContextHandler context = new ServletContextHandler(jettyServer, "/");
        context.addServlet(jerseyServlet, "/*");

        // Tells the Jersey Servlet which REST service/class to load.
        jerseyServlet.setInitParameter(
                "jersey.config.server.provider.classnames",
                Services.class.getCanonicalName());

        try {
            jettyServer.start();
            jettyServer.join();
        } finally{
            jettyServer.destroy();
        }
    }
}
