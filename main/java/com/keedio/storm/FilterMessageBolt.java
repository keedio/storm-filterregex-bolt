package com.keedio.storm;

import java.util.Date;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
        
        yammerAdapter = StormYammerMetricsAdapter.configure(stormConf, context, new MetricsRegistry());
        accepted = yammerAdapter.createCounter("accepted", "");
        rejected = yammerAdapter.createCounter("rejected", "");
        throughput = yammerAdapter.createHistogram("throughput", "", false);
		
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
				collector.emit(tuple(message.getBytes()));
				accepted.inc();
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
				collector.emit(tuple(message.getBytes()));
				accepted.inc();
			}else {
				rejected.inc();
				LOG.debug("NOT Emiting tuple(denied): " + message.toString());
			}
		}
		else{
			LOG.debug("Emiting tuple(no filter): " + message.toString());
			collector.emit(tuple(message.getBytes()));
			accepted.inc();
		}

	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

}
