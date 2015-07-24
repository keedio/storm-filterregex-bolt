package org.keedio.storm.bolt.filter;

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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import info.ganglia.gmetric4j.gmetric.GMetric;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.keedio.storm.bolt.filter.metrics.MetricsController;
import org.keedio.storm.bolt.filter.metrics.MetricsEvent;
import org.keedio.storm.bolt.filter.metrics.SimpleMetric;
import org.apache.log4j.*;
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
    private Map<String, Pattern> allPatterns;
    private OutputCollector collector;
    private int refreshTime;
	private MetricsController mc;

	//for Ganglia only
	private String hostGanglia, reportGanglia;
	private GMetric.UDPAddressingMode modeGanglia;
	private int portGanglia, ttlGanglia;
	private int minutesGanglia;
	
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
		
		if (stormConf.get("refreshtime") == null)
			refreshTime = 10;
		else
			refreshTime = Integer.parseInt((String) stormConf.get("refreshtime"));
		
		this.collector = collector;

		//check if in topology's config ganglia.report is set to "yes"
		if (loadGangliaProperties(stormConf)) {
			mc = new MetricsController(hostGanglia, portGanglia, modeGanglia, ttlGanglia, minutesGanglia);
		} else {
			mc = new MetricsController();
		}

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

	private Map<String, Pattern> getPropKeys(Map stormConf, String pattern) {
		Map<String,Pattern> set = new HashMap<String,Pattern>();
		Pattern pat = Pattern.compile(pattern + "\\." + "(\\w*)");
		
		Iterator<String> it = stormConf.keySet().iterator();
		
		while (it.hasNext()) {
			String key = it.next();
			Matcher m = pat.matcher(key);
			
			if (m.find()) {
				String value = (String)stormConf.get(key);
				set.put(m.group(1), Pattern.compile(value));
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
		
		boolean procesar = false;
		// Sin no existan blancas o negras se procesa
		if (allowMessages.isEmpty() && denyMessages.isEmpty()) {
			procesar = true;
		} else {
			// Si esta en la lista blanca se procesa
			if (!allowMessages.isEmpty()){
				Pattern patternAllow = Pattern.compile(allowMessages);
				Matcher matcherAllow = patternAllow.matcher(message);
				if (matcherAllow.find()) {
					LOG.debug("Emiting tuple(allowed): " + message.toString());
					procesar = true;
				} else {
					// Existe lista blanca pero no coincide
					LOG.debug("Emiting tuple(not allowed): " + message.toString());
					procesar = false;
				}
			} else {
				// Si esta en la lista negra no se procesa
				if (!denyMessages.isEmpty()){
					Pattern patternDeny = Pattern.compile(denyMessages);
					Matcher matcherDeny = patternDeny.matcher(message);
					if (!matcherDeny.find()) {
						// No esta en la lista negra. Se procesa
						LOG.debug("Emiting tuple(not denied): " + message.toString());
						procesar = true;
					} else {
						LOG.debug("Emiting tuple (denied): " + message.toString());
						procesar = false;
					}
				}
			}
		}
		
		
		if (procesar) {
			LOG.debug("Emiting tuple(no filter): " + message.toString());
			try {
				if (allPatterns.size() == 0) {
					// Pasamos el mensaje tal y como llega
					collector.emit(tuple(message));
					collector.ack(input);
					mc.manage(new MetricsEvent(MetricsEvent.INC_METER, "accepted"));
				} else{
					String nextMessage = filterMessage(message);
					if (nextMessage == null)
						collector.emit(tuple(message));
					else
						collector.emit(tuple(nextMessage));
					collector.ack(input);
					mc.manage(new MetricsEvent(MetricsEvent.INC_METER, "accepted"));
				}
			} catch (ParseException | InvocationTargetException | NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException e) {
				LOG.error("error al realizar el filtrado de datos");
				collector.reportError(e);
				collector.ack(input);
				mc.manage(new MetricsEvent(MetricsEvent.INC_METER, "rejected"));
			}
		} else {
			LOG.error("No se procesa el mensaje");
			collector.ack(input);
			mc.manage(new MetricsEvent(MetricsEvent.INC_METER, "rejected"));
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
		Map<String, String> map = null;
		
		while (it.hasNext()) {
			String key = it.next();
			
			LOG.debug("Trying regular expression: " + key);
			map = getFilteredMessages(key, originalMessage);
			
			if (map != null)
				break;
		}
		if (map == null) {
			// No se ha logrado el matching. enviamos el texto de origen
			return message;
		} else {
			obj.put("message", map);
		}
		
		return obj.toJSONString();
	}

	private Map<String, String> getFilteredMessages(String key, String message) throws ParseException, InvocationTargetException, NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException {
		
		Map<String, Map<String, String>> map = new HashMap<>();
		
		Pattern p = allPatterns.get(key);
		Map groups = getNamedGroups(p);
		
		Matcher m = p.matcher(message);
		
		if (groups.isEmpty()) {
			int j = 0;

			while (m.find()) {
				int count = m.groupCount();
								
				for (int i=1;i<=count;i++) {
					if (!map.containsKey("group" + j)) map.put("group" + j, new HashMap<String, String>());
					map.get("group" + j).put("item" + i, m.group(i));
				}
				
				j++;
			}

		} else {
			int i=0;
			while (m.find()) {
				Iterator<String> it = groups.keySet().iterator();
				
				while (it.hasNext()) {
					String k = it.next();
					
					if (!map.containsKey("group" + i)) map.put("group" + i, new HashMap<String, String>());
					map.get("group" + i).put(k, m.group(k));
				}
				
				i++;
			}
			

		}		

		// Añadimos mensaje a la metrica si se ha logrado procesar el mensaje
		if (map.keySet().size()>0) mc.manage(new MetricsEvent(MetricsEvent.INC_METER, key));

		// Se cambia para que solo se envía la primera de las coincidencias. Se quita
		// generalidad
		if (map.keySet().size() > 1) LOG.debug("Some items were discarded. Take care.");
		
		return (map.get("group0") == null)?null:map.get("group0");
				
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


	/**
	 * ganglia's server properties are taken from main topology's config
	 * @param stormConf
	 * @return
	 */
	private boolean loadGangliaProperties(Map stormConf){
		boolean loaded = false;
		reportGanglia = (String) stormConf.get("ganglia.report");
		if (reportGanglia.equals("yes")) {
			hostGanglia = (String) stormConf.get("ganglia.host");
			portGanglia = Integer.parseInt((String) stormConf.get("ganglia.port"));
			ttlGanglia = Integer.parseInt((String) stormConf.get("ganglia.ttl"));
			minutesGanglia = Integer.parseInt((String) stormConf.get("ganglia.minutes"));
			String stringModeGanglia = (String) stormConf.get("ganglia.UDPAddressingMode");
			modeGanglia = GMetric.UDPAddressingMode.valueOf(stringModeGanglia);
			loaded = true;
		}
	   return loaded;
	}

}
