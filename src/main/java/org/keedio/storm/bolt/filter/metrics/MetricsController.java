package org.keedio.storm.bolt.filter.metrics;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.log4j.*;
import org.keedio.storm.bolt.filter.FilterMessageBolt;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.UniformSnapshot;

import com.codahale.metrics.ganglia.GangliaReporter;


/**
 * This class represents the controller metrics to publish to the source.
 * Extends MonitoredCounterGroup class to allow the publication of JMX metrics
 * following the mechanism established by Flume.
 */
public class MetricsController implements Serializable {

    public static final Logger LOG = Logger
            .getLogger(MetricsController.class);
    private static final Pattern hostnamePattern =
            Pattern.compile("^[a-zA-Z0-9][a-zA-Z0-9-]*(\\.([a-zA-Z0-9][a-zA-Z0-9-]*))*$");


    // Ojo, problema de serializacion sin el transient
    protected transient MetricRegistry metrics;
    protected transient Map<String, Meter> meters;
    protected transient Histogram throughput;

    public MetricRegistry getMetrics() {
        return metrics;
    }

    public MetricsController() {

        metrics = new MetricRegistry();
        meters = new HashMap<String, Meter>();
        throughput = metrics.histogram("throughput");

        // Iniciamos el reporter de metricas
        JmxReporter reporter = JmxReporter.forRegistry(metrics).inDomain(metricsPath()).build();
        reporter.start();
        GangliaReporter gangliaReporter = GangliaReporter.forRegistry(metrics).build();
        gangliaReporter.start(5000L, TimeUnit.MILLISECONDS);

    }

    /**
     * This method manages metric based on events received.
     * <p>
     * For new metrics will need to create the corresponding event type in
     * MetricsEvent class and then define their behavior here
     *
     * @param event event to manage
     */
    public void manage(MetricsEvent event) {
        switch (event.getCode()) {
            case MetricsEvent.INC_METER:
                metrics.meter(event.getStr()).mark();
                break;
            case MetricsEvent.UPDATE_THROUGHPUT:
                throughput.update(event.getValue());
                break;
            case MetricsEvent.NEW_METRIC_METER:
                Meter meter = metrics.meter(event.getStr());
                meters.put(event.getStr(), meter);
                break;
        }
    }

    private String metricsPath() {
        final String myHostname = extractHostnameFromFQHN(detectHostname());
        return myHostname;
    }

    private static String detectHostname() {
        String hostname = "hostname-could-not-be-detected";
        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            LOG.error("Could not determine hostname");
        }
        return hostname;
    }

    private static String extractHostnameFromFQHN(String fqhn) {
        if (hostnamePattern.matcher(fqhn).matches()) {
            if (fqhn.contains(".")) {
                return fqhn.split("\\.")[0];
            } else {
                return fqhn;
            }
        } else {
            // We want to return the input as-is
            // when it is not a valid hostname/FQHN.
            return fqhn;
        }
    }


}
