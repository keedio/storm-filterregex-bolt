package org.keedio.storm.bolt.filter.metrics;

import java.io.IOException;
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

import info.ganglia.gmetric4j.gmetric.GMetric;
import info.ganglia.gmetric4j.gmetric.GMetric.*;


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

    private static final String GANGLIA_REPORT = "ganglia.report";
    private static final String GANGLIA_HOST = "ganglia.host";
    private static final String GANGLIA_PORT = "ganglia.port";
    private static final String GANGLIA_TTL = "ganglia.ttl";
    private static final String GANGLIA_PULL_SECONDS = "ganglia.pull.seconds";
    private static final String GANGLIA_MODE = "ganglia.UDPAddressingMode";    

    // Ojo, problema de serializacion sin el transient
    protected transient MetricRegistry metrics;
    protected transient Map<String, Meter> meters;
    protected transient Histogram throughput;

    public MetricRegistry getMetrics() {
        return metrics;
    }

    public MetricsController(Map configuration) {
        
    	metrics = new MetricRegistry();
        meters = new HashMap<String, Meter>();
        throughput = metrics.histogram("throughput");
        JmxReporter reporter = JmxReporter.forRegistry(metrics).inDomain(metricsPath()).build();
        reporter.start();
        
        if (configuration.containsKey(GANGLIA_REPORT) && configuration.get(GANGLIA_REPORT).equals("true")){
        	LOG.info("Ganglia reporting ENABLED by configuration");
        	startGangliaReporter(configuration);
        }
    }
    
    private void startGangliaReporter(Map configuration){
		
		String host = (String) configuration.get(GANGLIA_HOST);
		int port = Integer.valueOf((String)configuration.get(GANGLIA_PORT));
		int ttl = Integer.valueOf((String) configuration.get(GANGLIA_TTL));
		long pullSeconds = Long.valueOf((String)configuration.get(GANGLIA_PULL_SECONDS));
		
		UDPAddressingMode mode;
		String UDPmode = (String) configuration.get(GANGLIA_MODE);

		if (UDPmode == null || UDPmode.equalsIgnoreCase("unicast"))
			mode = UDPAddressingMode.UNICAST;
		else
			mode = UDPAddressingMode.MULTICAST;
		
		try {
	    	GMetric ganglia = new GMetric(host, port, mode, ttl);
	        GangliaReporter gangliaReporter = GangliaReporter.forRegistry(metrics)
	                .convertRatesTo(TimeUnit.SECONDS)
	                .convertDurationsTo(TimeUnit.MILLISECONDS)
	                .build(ganglia);
	        gangliaReporter.start(pullSeconds, TimeUnit.SECONDS);
		}catch (IOException e){
	        LOG.error("", e);
	    }
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
