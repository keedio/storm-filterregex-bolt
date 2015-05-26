package org.keedio.storm.spout.simple;

import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class SimpleSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;

	@Override
	public void nextTuple() {
		String ret = "{\"message\":\"fsfsdf\",\"extraData\":{\"nombre\":\"Rodrigo\",\"Apellidos\":\"Olmo\"}}";

		collector.emit(new Values(ret.getBytes()));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("data"));
	}

	@Override
	public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector arg2) {
		// TODO Auto-generated method stub
		this.collector = arg2;
	}

}