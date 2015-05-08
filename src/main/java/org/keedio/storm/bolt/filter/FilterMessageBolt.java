package org.keedio.storm.bolt.filter;

import info.ganglia.gmetric4j.gmetric.GMetric;
import info.ganglia.gmetric4j.gmetric.GMetric.UDPAddressingMode;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.keedio.storm.bolt.filter.metrics.MetricsController;
import org.keedio.storm.bolt.filter.metrics.MetricsEvent;
import org.keedio.storm.bolt.filter.metrics.SimpleMetric;
import org.apache.log4j.*;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ganglia.GangliaReporter;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import static backtype.storm.utils.Utils.tuple;

public class FilterMessageBolt extends BaseRichBolt {

	public static final Logger LOG = Logger
			.getLogger(FilterMessageBolt.class);
	private static final Pattern hostnamePattern =
		    Pattern.compile("^[a-zA-Z0-9][a-zA-Z0-9-]*(\\.([a-zA-Z0-9][a-zA-Z0-9-]*))*$");

	String allowMessages, denyMessages;
    private Date lastExecution = new Date();
    private String groupSeparator;
    private Map<String, String> allPatterns;
    private OutputCollector collector;
    private String gangliaServer;
    private int gangliaPort;
    private int refreshTime;
	private MetricsController mc;
 
	
    public MetricsController getMc() {
		return mc;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tupleValue"));

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		allowMessages = (String) stormConf.get("filter.bolt.allow");
		denyMessages = (String) stormConf.get("filter.bolt.deny");
		//pattern = (String) stormConf.get("pattern");
		groupSeparator = (String) stormConf.get("group.separator");
		allPatterns = getPropKeys(stormConf, "conf");
		gangliaServer = (String) stormConf.get("ganglia.server");
		gangliaPort = Integer.parseInt((String) stormConf.get("ganglia.port")); 
		refreshTime = Integer.parseInt((String) stormConf.get("refreshtime"));
		
		this.collector = collector;
		mc = new MetricsController();
		
		//Inicializamos las metricas para los diferentes filtros
		Iterator<String> keys = allPatterns.keySet().iterator();
		while (keys.hasNext()) {
			String key = keys.next();
			mc.manage(new MetricsEvent(MetricsEvent.NEW_METRIC_METER, key));
			
			// Registramos la metrica para su publicacion
			SimpleMetric metric = new SimpleMetric(mc.getMetrics(), key, SimpleMetric.TYPE_METER);
			context.registerMetric(key, metric, refreshTime);
		}
		
		// Y añadimos las metricas de rejected, accepted y throuput
		mc.manage(new MetricsEvent(MetricsEvent.NEW_METRIC_METER, "accepted"));
		mc.manage(new MetricsEvent(MetricsEvent.NEW_METRIC_METER, "rejected"));
		// Registramos la metrica para su publicacion
		SimpleMetric accepted = new SimpleMetric(mc.getMetrics(), "accepted", SimpleMetric.TYPE_METER);
		SimpleMetric rejected = new SimpleMetric(mc.getMetrics(), "rejected", SimpleMetric.TYPE_METER);
		SimpleMetric histogram = new SimpleMetric(mc.getMetrics(), "histogram", SimpleMetric.TYPE_HISTOGRAM);
		context.registerMetric("accepted", accepted, refreshTime);
		context.registerMetric("rejected", rejected, refreshTime);
		context.registerMetric("histogram", histogram, refreshTime);
		
	}

	private Map<String, String> getPropKeys(Map stormConf, String pattern) {
		Map<String,String> set = new HashMap<String,String>();
		Pattern pat = Pattern.compile(pattern + "\\." + "(\\w*)");
		
		Iterator<String> it = stormConf.keySet().iterator();
		
		while (it.hasNext()) {
			String key = it.next();
			Matcher m = pat.matcher(key);
			
			if (m.find()) {
				String value = (String)stormConf.get(key);
				set.put(m.group(1), value);
			}
				
		}

		return set;
	}

	@Override
	public void execute(Tuple input) {
		
		LOG.debug("FilterMessage: execute");

		// Añadimos al throughput e inicializamos el date
        Date actualDate = new Date();
        long aux = (actualDate.getTime() - lastExecution.getTime())/1000;
        lastExecution = actualDate;
        
        // Registramos para calculo de throughput
        mc.manage(new MetricsEvent(MetricsEvent.UPDATE_THROUGHPUT, aux));
		
		String message = new String(input.getBinary(0));
		
		if (!allowMessages.isEmpty()){
			Pattern patternAllow = Pattern.compile(allowMessages);
			Matcher matcherAllow = patternAllow.matcher(message);
			if (matcherAllow.find()) {
				LOG.debug("Emiting tuple(allowed): " + message.toString());
				try {
					String nextMessage = filterMessage(message);
					System.out.println(nextMessage);
					collector.emit(tuple(nextMessage));
					collector.ack(input);
					mc.manage(new MetricsEvent(MetricsEvent.INC_METER, "accepted"));
				} catch (ParseException | InvocationTargetException | NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException e) {
					LOG.error("error al realizar el filtrado de datos", e);
					collector.reportError(e);
					collector.ack(input);
					mc.manage(new MetricsEvent(MetricsEvent.INC_METER, "rejected"));
				}
			}else{
				LOG.debug("NOT Emiting tuple(not allowed): " + message.toString());
				collector.ack(input);
				mc.manage(new MetricsEvent(MetricsEvent.INC_METER, "rejected"));
			}
			
		}
		else if (!denyMessages.isEmpty()){
			Pattern patternDeny = Pattern.compile(denyMessages);
			Matcher matcherDeny = patternDeny.matcher(message);
			if (!matcherDeny.find()) {
				LOG.debug("Emiting tuple(not denied): " + message.toString());
				try {
					String nextMessage = filterMessage(message);
					System.out.println(nextMessage);
					collector.emit(tuple(nextMessage));
					collector.ack(input);
					mc.manage(new MetricsEvent(MetricsEvent.INC_METER, "accepted"));
				} catch (ParseException | InvocationTargetException | NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException e) {
					LOG.error("error al realizar el filtrado de datos", e);
					collector.reportError(e);
					collector.ack(input);
					mc.manage(new MetricsEvent(MetricsEvent.INC_METER, "rejected"));
				}
			}else {
				mc.manage(new MetricsEvent(MetricsEvent.INC_METER, "rejected"));
				collector.ack(input);
				LOG.debug("NOT Emiting tuple(denied): " + message.toString());
			}
		}
		else{
			LOG.debug("Emiting tuple(no filter): " + message.toString());
			try {
				String nextMessage = filterMessage(message);
				System.out.println(nextMessage);
				collector.emit(tuple(nextMessage));
				collector.ack(input);
				mc.manage(new MetricsEvent(MetricsEvent.INC_METER, "accepted"));
			} catch (ParseException | InvocationTargetException | NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException e) {
				LOG.error("error al realizar el filtrado de datos", e);
				collector.reportError(e);
				collector.ack(input);
				mc.manage(new MetricsEvent(MetricsEvent.INC_METER, "rejected"));
			}
		}

	}

	public String filterMessage(String message) throws ParseException, InvocationTargetException, NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException {
		
		JSONParser parser = new JSONParser();  
		
		// El mensaje recibido es del tipo {"extraData":"...", "message":"..."}
		JSONObject obj = (JSONObject)parser.parse(message);
		// Aplicamos el filtro para obtener map con resultados
		String originalMessage = (String) obj.get("message");
		StringBuffer buf = new StringBuffer();
		Iterator<String> it = allPatterns.keySet().iterator();
		
		while (it.hasNext()) {
			String key = it.next();
			
			String all = getFilteredMessages(key, originalMessage);
			
			buf.append(all).append(groupSeparator);
		}
		
		obj.put("message", buf.toString());
		
		return obj.toJSONString();
	}

	private String getFilteredMessages(String key, String message) throws ParseException, InvocationTargetException, NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException {
		
		Map<String, Map<String, String>> map = new HashMap<>();
		
		String pattern = allPatterns.get(key);
		Pattern p = Pattern.compile(pattern);
		Map groups = getNamedGroups(p);
		
		Matcher m = p.matcher(message);
		
		if (groups.isEmpty()) {
			int j = 0;

			while (m.find()) {
				int count = m.groupCount();
				
				// Añadimos mensaje a la metrica
				mc.manage(new MetricsEvent(MetricsEvent.INC_METER, key));
				
				for (int i=1;i<=count;i++) {
					if (!map.containsKey("group" + j)) map.put("group" + j, new HashMap<String, String>());
					map.get("group" + j).put("item" + i, m.group(i));
				}
				
				j++;
			}

			return JSONObject.toJSONString(map);
		} else {
			int i=0;
			while (m.find()) {
				Iterator<String> it = groups.keySet().iterator();
				
				// Añadimos mensaje a la metrica
				mc.manage(new MetricsEvent(MetricsEvent.INC_METER, key));

				while (it.hasNext()) {
					String k = it.next();
					
					if (!map.containsKey("group" + i)) map.put("group" + i, new HashMap<String, String>());
					map.get("group" + i).put(k, m.group(k));
				}
				
				i++;
			}
			
			return JSONObject.toJSONString(map);

		}		
				
	}
	
	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}
	
	private String metricsPath() {
		final String myHostname = extractHostnameFromFQHN(detectHostname());
		return myHostname;
	}

	private static String detectHostname() {
		String hostname = "hostname-could-not-be-detected";
		try {
			hostname = InetAddress.getLocalHost().getHostName();
		}
		catch (UnknownHostException e) {
			LOG.error("Could not determine hostname");
		}
		return hostname;
	}

	private static String extractHostnameFromFQHN(String fqhn) {
		if (hostnamePattern.matcher(fqhn).matches()) {
			if (fqhn.contains(".")) {
				return fqhn.split("\\.")[0];
			}
			else {
				return fqhn;
			}
		}
		else {
			// We want to return the input as-is
			// when it is not a valid hostname/FQHN.
			return fqhn;
		}
	}
	
	private static Map<String, Integer> getNamedGroups(Pattern regex)
			throws NoSuchMethodException, SecurityException,
			IllegalAccessException, IllegalArgumentException,
			InvocationTargetException {

		Method namedGroupsMethod = Pattern.class.getDeclaredMethod("namedGroups");
		namedGroupsMethod.setAccessible(true);

		Map<String, Integer> namedGroups = null;
		namedGroups = (Map<String, Integer>) namedGroupsMethod.invoke(regex);

		if (namedGroups == null) {
			throw new InternalError();
		}

		return Collections.unmodifiableMap(namedGroups);
	}

	

}
