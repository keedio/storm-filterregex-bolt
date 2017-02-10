package org.keedio.storm.bolt.filter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.log4j.*;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
//import org.codehaus.jackson.map.ObjectMapper;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;

public class FilterMessageBolt extends BaseRichBolt {

	private static final long serialVersionUID = 4573441227107793181L;

	public static final Logger LOG = Logger
			.getLogger(FilterMessageBolt.class);

	private List<String> whiteListRegex, blackListRegex;
	private boolean isEnriched = false;
    private OutputCollector collector;

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	    declarer.declareStream("success", new Fields("bytes"));
	    declarer.declareStream("fail", new Fields("bytes"));
		declarer.declare(new Fields("bytes"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return super.getComponentConfiguration();
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		whiteListRegex = new ArrayList<String>();
		blackListRegex = new ArrayList<String>();
		
		int i=1;
		while (stormConf.get("filter.bolt.whitelist."+i)!=null){
			whiteListRegex.add((String) stormConf.get("filter.bolt.whitelist."+i));
			i++;
		}
		
		if (whiteListRegex.isEmpty()){
			i=1;
			while (stormConf.get("filter.bolt.blacklist."+i)!=null){
				blackListRegex.add((String) stormConf.get("filter.bolt.blacklist."+i));
				i++;
			}
		}
		
		if (stormConf.get("filter.bolt.enriched") != null && stormConf.get("filter.bolt.enriched").equals("true"))
			isEnriched=true;		
		
		this.collector = collector;		
	}

	@Override
	public void execute(Tuple input) {
		
		boolean emitTuple;
        		
		String message = new String(input.getBinary(0));
		
		if(isEnriched){
			try{
				//Map<String,String> inputJson = gson.fromJson(message,Map.class); 	    
				//message = inputJson.get("message");
				Gson gson = new GsonBuilder().create();
				message = (String) gson.fromJson(message,Map.class).get("message");
				if (message == null){
					LOG.warn("Message (" + message + ") not in Keedio Enriched format");
					collector.emit("fail",input.getValues());
					collector.ack(input);
				}
			}catch (JsonSyntaxException e){
				LOG.warn("Not valid Json. Message (" + message + ") not in Keedio Enriched format",e);
				collector.emit("fail",input.getValues());
				collector.ack(input);
			}
		}

		if (whiteListRegex.isEmpty() && blackListRegex.isEmpty()) {
			emitTuple = true;
		} else {
			if (!whiteListRegex.isEmpty()){
				emitTuple = false;
				int i=0;
				while (!emitTuple && i<whiteListRegex.size()){
					Pattern whiteListPattern = Pattern.compile(whiteListRegex.get(i));
					if (whiteListPattern.matcher(message).find()){
						emitTuple = true;
					}
					i++;
				}
			}else {
				emitTuple = true;
				int i=0;
				while (emitTuple && i<blackListRegex.size()){
					Pattern blackListPattern = Pattern.compile(blackListRegex.get(i));
					if (blackListPattern.matcher(message).find()){
						emitTuple = false;
					}
					i++;
				}
			}
		}
				
		if (emitTuple){
			collector.emit("success",input.getValues());
			collector.emit(input.getValues());
			collector.ack(input);
		}else{
			collector.emit("fail",input.getValues());
			collector.ack(input);
		}
	}

	@Override
	public void cleanup() {
	}
}