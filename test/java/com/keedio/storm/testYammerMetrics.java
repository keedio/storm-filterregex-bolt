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

public class testYammerMetrics {

	ServerSocket ss;
	private FilterMessageBolt bolt;
	
	@Mock
	private TopologyContext topologyContext = mock(TopologyContext.class);

	@Mock
	private BasicOutputCollector collector = mock(BasicOutputCollector.class);

	@Before
	public void setUp() throws UnknownHostException, IOException {
		ss = new ServerSocket(8888);
		bolt = new FilterMessageBolt();
		Config conf = new Config();
		conf.put("filter.bolt.allow", "\\d{3}"); // Aceptamos las cadenas con tres digitos seguidos
		conf.put("filter.bolt.deny", "\\d{2}.{2}\\d"); // Rechazamos las cadenas que 
		bolt.prepare(conf, topologyContext);
	}

	@After
	public void finish() throws IOException {
		ss.close();
	}
	
	@Test
	public void testThroughput() throws InterruptedException {
		Tuple tuple = mock(Tuple.class);
	    when(tuple.getBinary(anyInt())).thenReturn("Output fake".getBytes());
		bolt.execute(tuple, collector);
		bolt.execute(tuple, collector);
		bolt.execute(tuple, collector);
		bolt.execute(tuple, collector);
		bolt.execute(tuple, collector);
		
		Assert.assertTrue("Se han capturado cuatro observaciones", bolt.getThroughput().getSnapshot().getValues().length == 5);
		Assert.assertTrue("La media esta por debajo de los 3 segundos", bolt.getThroughput().mean() < 1);

	}
	
	@Test
	public void testAccepted() throws InterruptedException {
		Tuple tuple = mock(Tuple.class);
	    when(tuple.getBinary(anyInt())).thenReturn("ww erwrw 222 fsfsdfsf".getBytes());
		bolt.execute(tuple, collector);
		
		Assert.assertTrue("Se acepta", bolt.getAccepted().count() == 1);

	}
	
	@Test
	public void testRejected1() throws InterruptedException {
		Tuple tuple = mock(Tuple.class);
	    when(tuple.getBinary(anyInt())).thenReturn("ww erwrw 222 fsfsdfsf".getBytes());
		bolt.execute(tuple, collector);
		
		Assert.assertTrue("Se rechazan", bolt.getRejected().count() == 0);

	}

	@Test
	public void testRejected2() throws InterruptedException {
		Tuple tuple = mock(Tuple.class);
	    when(tuple.getBinary(anyInt())).thenReturn("ww erwrw (22xg3)nn fsfsdfsf".getBytes());
		bolt.execute(tuple, collector);
		
		Assert.assertTrue("Se rechazan", bolt.getRejected().count() == 1);

	}

}
