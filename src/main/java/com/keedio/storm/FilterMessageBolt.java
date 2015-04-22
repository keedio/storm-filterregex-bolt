package com.keedio.storm;

import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.staslev.storm.metrics.yammer.StormYammerMetricsAdapter;
import com.github.staslev.storm.metrics.yammer.YammerFacadeMetric;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.MetricsRegistry;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import static backtype.storm.utils.Utils.tuple;

public class FilterMessageBolt implements IBasicBolt {

	public static final Logger LOG = LoggerFactory
			.getLogger(FilterMessageBolt.class);

	String allowMessages, denyMessages;
    private Date lastExecution = new Date();
    // Declaramos el adaptador y las metricas de yammer
    private StormYammerMetricsAdapter yammerAdapter;
	private Counter rejected;
	private Counter accepted;
    private Histogram throughput;   
    private String groupSeparator;
    //private String pattern;
    private Map<String, String> allPatterns;
    
	public Counter getRejected() {
		return rejected;
	}

	public void setRejected(Counter rejected) {
		this.rejected = rejected;
	}

	public Counter getAccepted() {
		return accepted;
	}

	public void setAccepted(Counter accepted) {
		this.accepted = accepted;
	}

	public Histogram getThroughput() {
		return throughput;
	}

	public void setThroughput(Histogram throughput) {
		this.throughput = throughput;
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
	public void prepare(Map stormConf, TopologyContext context) {
		allowMessages = (String) stormConf.get("filter.bolt.allow");
		denyMessages = (String) stormConf.get("filter.bolt.deny");
		//pattern = (String) stormConf.get("pattern");
		groupSeparator = (String) stormConf.get("group.separator");
		allPatterns = getPropKeys(stormConf, "conf");
        
        yammerAdapter = StormYammerMetricsAdapter.configure(stormConf, context, new MetricsRegistry());
        accepted = yammerAdapter.createCounter("accepted", "");
        rejected = yammerAdapter.createCounter("rejected", "");
        throughput = yammerAdapter.createHistogram("throughput", "", false);
		
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
	public void execute(Tuple input, BasicOutputCollector collector) {
		
		LOG.debug("FilterMessage: execute");

		// Añadimos al throughput e inicializamos el date
        Date actualDate = new Date();
        long aux = (actualDate.getTime() - lastExecution.getTime())/1000;
        lastExecution = actualDate;
        
        // Registramos para calculo de throughput
        throughput.update(aux);
		
		String message = new String(input.getBinary(0));
		
		if (!allowMessages.isEmpty()){
			Pattern patternAllow = Pattern.compile(allowMessages);
			Matcher matcherAllow = patternAllow.matcher(message);
			if (matcherAllow.find()) {
				LOG.debug("Emiting tuple(allowed): " + message.toString());
				try {
					String nextMessage = filterMessage(message);
					System.out.println(nextMessage);
					collector.emit(tuple(nextMessage.getBytes()));
					accepted.inc();
				} catch (ParseException e) {
					LOG.error("error al realizar el filtrado de datos", e);
					rejected.inc();
				}
			}else{
				LOG.debug("NOT Emiting tuple(not allowed): " + message.toString());
				rejected.inc();
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
					collector.emit(tuple(nextMessage.getBytes()));
					accepted.inc();
				} catch (ParseException e) {
					LOG.error("error al realizar el filtrado de datos", e);
					rejected.inc();
				}
			}else {
				rejected.inc();
				LOG.debug("NOT Emiting tuple(denied): " + message.toString());
			}
		}
		else{
			LOG.debug("Emiting tuple(no filter): " + message.toString());
			try {
				String nextMessage = filterMessage(message);
				System.out.println(nextMessage);
				collector.emit(tuple(nextMessage.getBytes()));
				accepted.inc();
			} catch (ParseException e) {
				LOG.error("error al realizar el filtrado de datos", e);
				rejected.inc();
			}
		}

	}

	public String filterMessage(String message) throws ParseException {
		
		JSONParser parser = new JSONParser();  
		
		// El mensaje recibido es del tipo {"extraData":"...", "message":"..."}
		JSONObject obj = (JSONObject)parser.parse(message);
		// Aplicamos el filtro para obtener map con resultados
		String originalMessage = (String) obj.get("message");
		StringBuffer buf = new StringBuffer();
		Iterator<String> it = allPatterns.keySet().iterator();
		
		while (it.hasNext()) {
			String key = it.next();
			String pattern = allPatterns.get(key);
			
			String all = getFilteredMessages(pattern, originalMessage);
			
			buf.append(all).append(groupSeparator);
		}
		
		obj.put("message", buf.toString());
		
		return obj.toJSONString();
	}

	private String getFilteredMessages(String pattern, String message) throws ParseException {

		Pattern p = Pattern.compile(pattern);
		Matcher m = p.matcher(message);
		
		StringBuffer buf = new StringBuffer();
		if (m.find()) {
			int count = m.groupCount();
			
			for (int i=1;i<=count;i++) {
				buf.append(m.group(i));
				if (i != count)
					buf.append(groupSeparator);
			}
		}
		

		return buf.toString();
	}
	
	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

}
