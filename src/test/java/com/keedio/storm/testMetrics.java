package com.keedio.storm;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.UnknownHostException;

import org.junit.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.mockito.Mockito.*;
import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.tuple.Tuple;

public class testMetrics {

	ServerSocket ss;
	private FilterMessageBolt bolt;
	
	@Mock
	private TopologyContext topologyContext = mock(TopologyContext.class);

	@Mock
	private OutputCollector collector = mock(OutputCollector.class);

	@Before
	public void setUp() throws UnknownHostException, IOException {
		ss = new ServerSocket(8888);
		bolt = new FilterMessageBolt();
		Config conf = new Config();
		conf.put("filter.bolt.allow", ""); // Aceptamos las cadenas con tres digitos seguidos
		conf.put("filter.bolt.deny", "\\d{3}"); // Rechazamos las cadenas que 
		conf.put("metrics.reporter.yammer.facade..metric.bucket.seconds", 10);
		conf.put("conf.pattern1", "(<date>[^\\s]+)\\s+");
		conf.put("group.separator", "|");
		bolt.prepare(conf, topologyContext, collector);
	}

	@After
	public void finish() throws IOException {
		ss.close();
	}
	
	@Test
	public void testThroughput() throws InterruptedException {
		String cad = "{\"extraData\":\"fsfsdf\",\"message\":\"Fake\"}";
		
		Tuple tuple = mock(Tuple.class);
	    when(tuple.getBinary(anyInt())).thenReturn(cad.getBytes());
		bolt.execute(tuple);
		bolt.execute(tuple);
		bolt.execute(tuple);
		bolt.execute(tuple);
		bolt.execute(tuple);
		
		Assert.assertTrue("Se han capturado cuatro observaciones", bolt.getMc().getMetrics().histogram("throughput").getSnapshot().getValues().length == 5);
		Assert.assertTrue("La media esta por debajo de los 3 segundos", bolt.getMc().getMetrics().histogram("throughput").getSnapshot().getMean() < 1);

	}
	
	@Test
	public void testAccepted() throws InterruptedException {
		String ret = "{\"extraData\":\"fsfsdf\",\"message\":\"ww erwrw 22 fsfsdfsf\"}";		
		
		Tuple tuple = mock(Tuple.class);
	    when(tuple.getBinary(anyInt())).thenReturn(ret.getBytes());
		bolt.execute(tuple);
		
		Assert.assertTrue("Se acepta", bolt.getMc().getMetrics().meter("accepted").getCount() == 1);

	}
	
	@Test
	public void testRejected1() throws InterruptedException {
		String ret = "{\"extraData\":\"fsfsdf\",\"message\":\"ww erwrw 222 fsfsdfsf\"}";		
		
		Tuple tuple = mock(Tuple.class);
	    when(tuple.getBinary(anyInt())).thenReturn(ret.getBytes());
		bolt.execute(tuple);
		
		Assert.assertTrue("Se rechazan", bolt.getMc().getMetrics().meter("rejected").getCount() == 1);

	}

	@Test
	public void testRejected2() throws InterruptedException {
		String ret = "{\"extraData\":\"fsfsdf\",\"message\":\"ww erwrw (22xg3)nn fsfsdfsf\"}";		
		
		Tuple tuple = mock(Tuple.class);
	    when(tuple.getBinary(anyInt())).thenReturn(ret.getBytes());
		bolt.execute(tuple);
		
		Assert.assertTrue("Se rechazan", bolt.getMc().getMetrics().meter("rejected").getCount() == 0);

	}

}
