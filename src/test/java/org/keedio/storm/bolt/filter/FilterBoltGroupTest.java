package org.keedio.storm.bolt.filter;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.UnknownHostException;

import org.junit.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.keedio.storm.bolt.filter.FilterMessageBolt;
import org.mockito.Mock;

import static org.mockito.Mockito.*;
import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.tuple.Tuple;

public class FilterBoltGroupTest {

	ServerSocket ss;
	private FilterMessageBolt bolt;
	
	@Mock
	private TopologyContext topologyContext = mock(TopologyContext.class);

	@Mock
	private OutputCollector collector = mock(OutputCollector.class);

	@Before
	public void setUp() throws UnknownHostException, IOException {
		bolt = new FilterMessageBolt();
		Config conf = new Config();
		conf.put("filter.bolt.allow", ""); // Aceptamos las cadenas con tres digitos seguidos
		conf.put("filter.bolt.deny", ""); // Rechazamos las cadenas que 

		conf.put("ganglia.report", "yes");
		conf.put("ganglia.host", "localhost");
		conf.put("ganglia.port", "5555");
		conf.put("ganglia.ttl", "1");
		conf.put("ganglia.minutes", "1");
		conf.put("ganglia.UDPAddressingMode", "MULTICAST");

		conf.put("refreshtime", "5"); // Rechazamos las cadenas que 
		conf.put("metrics.reporter.yammer.facade..metric.bucket.seconds", 10);
		conf.put("conf.pattern1", "(?<date>[4,5]\\d-[0,1]\\d-\\d\\d\\d\\d)\\s+(?<time>[0,1,2]\\d:\\d\\d)\\s+(?<time2>[0,1,2]\\d:\\d\\d)\\s+");
		conf.put("conf.pattern2", "(?<date>[0,1,2,3]\\d-[0,1]\\d-\\d\\d\\d\\d)\\s+(?<time>[0,1,2]\\d:\\d\\d)\\s+(?<time2>[0,1,2]\\d:\\d\\d)\\s+");
		conf.put("conf.pattern3", "(?<date>[0,1,2,3]\\d-[0,1]\\d-\\d\\d\\d\\d)\\s+(?<time>[0,1,2]\\d:\\d\\d)\\s+(?<time2>[0,1,2]\\d:\\d\\d)\\s+");
		conf.put("group.separator", "|");
		bolt.prepare(conf, topologyContext, collector);
	}

	@After
	public void finish() throws IOException {
	}
	
	@Test
	public void testGroupMessageParseFirst() {
		
		Tuple tuple = mock(Tuple.class);
		
		String ret = "{\"extraData\":\"fsfsdf\",\"message\":\"42-10-2015 25:15 22:22 gjslkgjs gklg jsdlkgsdfgsd 12-11-2015 22:22 22:22 fsdofsfjsl\"}";
		
	    when(tuple.getBinary(anyInt())).thenReturn(ret.getBytes());
		bolt.execute(tuple);
		
		Assert.assertTrue("Se acepta", bolt.getMc().getMetrics().meter("pattern1").getCount() == 1);
	}

	@Test
	public void testGroupMessageParseSecond() {
		
		Tuple tuple = mock(Tuple.class);
		
		String ret = "{\"extraData\":\"fsfsdf\",\"message\":\"02-10-2015 25:15 22:22 gjslkgjs gklg jsdlkgsdfgsd 12-11-2015 22:22 22:22 fsdofsfjsl\"}";
		
	    when(tuple.getBinary(anyInt())).thenReturn(ret.getBytes());
		bolt.execute(tuple);
		
		Assert.assertTrue("Se acepta", bolt.getMc().getMetrics().meter("pattern1").getCount() == 0);
		Assert.assertTrue("Se acepta", bolt.getMc().getMetrics().meter("pattern2").getCount() == 1);
	}

}
