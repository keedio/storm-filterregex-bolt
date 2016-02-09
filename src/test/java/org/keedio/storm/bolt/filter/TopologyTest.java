package org.keedio.storm.bolt.filter;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.keedio.storm.spout.simple.SimpleSpout;

import backtype.storm.Config;
import backtype.storm.ILocalCluster;
import backtype.storm.Testing;
import backtype.storm.generated.StormTopology;
import backtype.storm.testing.CompleteTopologyParam;
import backtype.storm.testing.MkClusterParam;
import backtype.storm.testing.MockedSources;
import backtype.storm.testing.TestJob;
import backtype.storm.testing.TupleCaptureBolt;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class TopologyTest {
	
	ServerSocket socket;
	
	@Before
	public void setUp() {
		try {
			socket = new ServerSocket(1234);
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}
	
	@After
	public void finish() {
		try {
			socket.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Test
	public void testSinFiltradoYConPatron() {
		

		MkClusterParam mkClusterParam = new MkClusterParam();
		mkClusterParam.setSupervisors(4);
		Config daemonConf = new Config();
		daemonConf.put(Config.STORM_LOCAL_MODE_ZMQ, false);
		mkClusterParam.setDaemonConf(daemonConf);

		/**
		 * This is a combination of <code>Testing.withLocalCluster</code> and <code>Testing.withSimulatedTime</code>.
		 */
		Testing.withSimulatedTimeLocalCluster(mkClusterParam, new TestJob() {
			@Override
			public void run(ILocalCluster cluster) {

				String ret = "{\"extraData\":{\"nombre\":\"Rodrigo\"},\"message\":\"hola amigo 11-03-2024 22:22 sflhsldfjs 11-11-11  11:11:11 \"}";
				
				// build the test topology
				TopologyBuilder builder = new TopologyBuilder();
				builder.setSpout("1", new SimpleSpout(), 3);
				builder.setBolt("3", new FilterMessageBolt(), 4).shuffleGrouping("1");

				StormTopology topology = builder.createTopology();

				// complete the topology

				// prepare the mock data
				MockedSources mockedSources = new MockedSources();
				mockedSources.addMockData("1", new Values(ret.getBytes()));

				// prepare the config
				Config conf = new Config();
				conf.setNumWorkers(2);
				conf.put("filter.bolt.allow", ""); // Aceptamos las cadenas con tres digitos seguidos
				conf.put("filter.bolt.deny", ""); // Rechazamos las cadenas que

				conf.put("ganglia.report", "yes");
				conf.put("ganglia.host", "localhost");
				conf.put("ganglia.port", "5555");
				conf.put("ganglia.ttl", "1");
				conf.put("ganglia.minutes", "1");
				conf.put("ganglia.UDPAddressingMode", "UNICAST");

				conf.put("refreshtime", "5"); // Rechazamos las cadenas que 
				conf.put("metrics.reporter.yammer.facade..metric.bucket.seconds", 10);
				conf.put("conf.pattern1", "(?<date>[0,1,2,3]\\d-[0,1]\\d-\\d\\d\\d\\d)\\s+(?<time>[0,1,2]\\d:\\d\\d)\\s+");
				//conf.put("conf.pattern2", "(<date>[^\\s]+)\\s+");
				conf.put("group.separator", "|");
//				conf.put("gangliaserver", "localhost");
//				conf.put("gangliaport", "1234");
				
				CompleteTopologyParam completeTopologyParam = new CompleteTopologyParam();
				completeTopologyParam.setMockedSources(mockedSources);
				completeTopologyParam.setStormConf(conf);
				/**
				 * TODO
				 */
				Map result = Testing.completeTopology(cluster, topology,
						completeTopologyParam);

				String tuple = Testing.readTuples(result, "3").get(0).toString();
				
				System.err.println(tuple);

				// El mensaje recibido es del tipo {"extraData":"...", "message":"..."}
				JSONParser parser = new JSONParser();
				JSONObject obj = null;
				try {
					obj = (JSONObject) parser.parse(tuple);
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				JSONObject o2 = (JSONObject)obj.get("message");
				Assert.assertNotNull("Existe el elemento fecha", o2.get("date"));
				Assert.assertNotNull("Existe el elemento hora", o2.get("time"));
			}

		});
	}

	@Test
	public void testSinFiltradoConPatronTextoNoCoincide() {
		

		MkClusterParam mkClusterParam = new MkClusterParam();
		mkClusterParam.setSupervisors(4);
		Config daemonConf = new Config();
		daemonConf.put(Config.STORM_LOCAL_MODE_ZMQ, false);
		mkClusterParam.setDaemonConf(daemonConf);

		/**
		 * This is a combination of <code>Testing.withLocalCluster</code> and <code>Testing.withSimulatedTime</code>.
		 */
		Testing.withSimulatedTimeLocalCluster(mkClusterParam, new TestJob() {
			@Override
			public void run(ILocalCluster cluster) {

				String ret = "{\"extraData\":{\"nombre\":\"Rodrigo\"},\"message\":\"hola amigo 11-03-2024 22:22 sflhsldfjs 11-11-11  11:11:11 \"}";
				
				// build the test topology
				TopologyBuilder builder = new TopologyBuilder();
				builder.setSpout("1", new SimpleSpout(), 3);
				builder.setBolt("3", new FilterMessageBolt(), 4).shuffleGrouping("1");

				StormTopology topology = builder.createTopology();

				// complete the topology

				// prepare the mock data
				MockedSources mockedSources = new MockedSources();
				mockedSources.addMockData("1", new Values(ret.getBytes()));

				// prepare the config
				Config conf = new Config();
				conf.setNumWorkers(2);
				conf.put("filter.bolt.allow", ""); // Aceptamos las cadenas con tres digitos seguidos
				conf.put("filter.bolt.deny", ""); // Rechazamos las cadenas que

				conf.put("ganglia.report", "yes");
				conf.put("ganglia.host", "localhost");
				conf.put("ganglia.port", "5555");
				conf.put("ganglia.ttl", "1");
				conf.put("ganglia.minutes", "1");
				conf.put("ganglia.UDPAddressingMode", "UNICAST");

				conf.put("refreshtime", "5"); // Rechazamos las cadenas que 
				conf.put("metrics.reporter.yammer.facade..metric.bucket.seconds", 10);
				conf.put("conf.pattern1", "(?<date>[0,1,2,3]\\d-[0,1]\\d-\\d\\d)\\s+(?<time>[0,1,2]\\d:\\d\\d)\\s+");
				//conf.put("conf.pattern2", "(<date>[^\\s]+)\\s+");
				conf.put("group.separator", "|");
//				conf.put("gangliaserver", "localhost");
//				conf.put("gangliaport", "1234");
				
				CompleteTopologyParam completeTopologyParam = new CompleteTopologyParam();
				completeTopologyParam.setMockedSources(mockedSources);
				completeTopologyParam.setStormConf(conf);
				/**
				 * TODO
				 */
				Map result = Testing.completeTopology(cluster, topology,
						completeTopologyParam);

				List tupleValues = (ArrayList)Testing.readTuples(result, "3").get(0);

				// El mensaje recibido es del tipo {"extraData":"...", "message":"..."}
				JSONParser parser = new JSONParser();
				JSONObject obj = null;
				try {
					obj = (JSONObject) parser.parse((String)tupleValues.get(0));
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				String o2 = (String)obj.get("message");
				Assert.assertTrue("Existe el elemento fecha", "hola amigo 11-03-2024 22:22 sflhsldfjs 11-11-11  11:11:11 ".equals(o2));
			}

		});
	}
	
	@Test
	public void testSinFiltradoPasaSegundoPatron() {
		

		MkClusterParam mkClusterParam = new MkClusterParam();
		mkClusterParam.setSupervisors(4);
		Config daemonConf = new Config();
		daemonConf.put(Config.STORM_LOCAL_MODE_ZMQ, false);
		mkClusterParam.setDaemonConf(daemonConf);

		/**
		 * This is a combination of <code>Testing.withLocalCluster</code> and <code>Testing.withSimulatedTime</code>.
		 */
		Testing.withSimulatedTimeLocalCluster(mkClusterParam, new TestJob() {
			@Override
			public void run(ILocalCluster cluster) {

				String ret = "{\"extraData\":{\"nombre\":\"Rodrigo\"},\"message\":\"hola amigo 11-03-2024 22:22 sflhsldfjs 11-11-11  11:11:11 \"}";
				
				// build the test topology
				TopologyBuilder builder = new TopologyBuilder();
				builder.setSpout("1", new SimpleSpout(), 3);
				builder.setBolt("3", new FilterMessageBolt(), 4).shuffleGrouping("1");

				StormTopology topology = builder.createTopology();

				// complete the topology

				// prepare the mock data
				MockedSources mockedSources = new MockedSources();
				mockedSources.addMockData("1", new Values(ret.getBytes()));

				// prepare the config
				Config conf = new Config();
				conf.setNumWorkers(2);
				conf.put("filter.bolt.allow", ""); // Aceptamos las cadenas con tres digitos seguidos
				conf.put("filter.bolt.deny", ""); // Rechazamos las cadenas que

				conf.put("ganglia.report", "yes");
				conf.put("ganglia.host", "localhost");
				conf.put("ganglia.port", "5555");
				conf.put("ganglia.ttl", "1");
				conf.put("ganglia.minutes", "1");
				conf.put("ganglia.UDPAddressingMode", "UNICAST");

				conf.put("refreshtime", "5"); // Rechazamos las cadenas que 
				conf.put("metrics.reporter.yammer.facade..metric.bucket.seconds", 10);
				conf.put("conf.pattern1", "(?<date>[0,1,2,3]\\d-[0,1]\\d-\\d\\d)\\s+(?<time>[0,1,2]\\d:\\d\\d)\\s+");
				conf.put("conf.pattern2", "(?<date>[0,1,2,3]\\d-[0,1]\\d-\\d\\d\\d\\d)\\s+(?<time>[0,1,2]\\d:\\d\\d)\\s+");
				conf.put("group.separator", "|");
//				conf.put("gangliaserver", "localhost");
//				conf.put("gangliaport", "1234");
				
				CompleteTopologyParam completeTopologyParam = new CompleteTopologyParam();
				completeTopologyParam.setMockedSources(mockedSources);
				completeTopologyParam.setStormConf(conf);
				/**
				 * TODO
				 */
				Map result = Testing.completeTopology(cluster, topology,
						completeTopologyParam);

				List tupleValues = (ArrayList)Testing.readTuples(result, "3").get(0);

				// El mensaje recibido es del tipo {"extraData":"...", "message":"..."}
				JSONParser parser = new JSONParser();
				JSONObject obj = null;
				try {
					obj = (JSONObject) parser.parse((String)tupleValues.get(0));
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				JSONObject o2 = (JSONObject)obj.get("message");
				Assert.assertNotNull("Existe el elemento fecha", o2.get("date"));
				Assert.assertNotNull("Existe el elemento hora", o2.get("time"));
			}

		});
	}
	
	
	@Test
	public void testSinFiltradoSinPatron() {
		

		MkClusterParam mkClusterParam = new MkClusterParam();
		mkClusterParam.setSupervisors(4);
		Config daemonConf = new Config();
		daemonConf.put(Config.STORM_LOCAL_MODE_ZMQ, false);
		mkClusterParam.setDaemonConf(daemonConf);

		/**
		 * This is a combination of <code>Testing.withLocalCluster</code> and <code>Testing.withSimulatedTime</code>.
		 */
		Testing.withSimulatedTimeLocalCluster(mkClusterParam, new TestJob() {
			@Override
			public void run(ILocalCluster cluster) {

				String ret = "{\"extraData\":{\"nombre\":\"Rodrigo\"},\"message\":\"hola amigo 11-03-2024 22:22 sflhsldfjs 11-11-11  11:11:11 \"}";
				
				// build the test topology
				TopologyBuilder builder = new TopologyBuilder();
				builder.setSpout("1", new SimpleSpout(), 3);
				builder.setBolt("3", new FilterMessageBolt(), 4).shuffleGrouping("1");

				StormTopology topology = builder.createTopology();

				// complete the topology

				// prepare the mock data
				MockedSources mockedSources = new MockedSources();
				mockedSources.addMockData("1", new Values(ret.getBytes()));

				// prepare the config
				Config conf = new Config();
				conf.setNumWorkers(2);
				conf.put("filter.bolt.allow", ""); // Aceptamos las cadenas con tres digitos seguidos
				conf.put("filter.bolt.deny", ""); // Rechazamos las cadenas que

				conf.put("ganglia.report", "yes");
				conf.put("ganglia.host", "localhost");
				conf.put("ganglia.port", "5555");
				conf.put("ganglia.ttl", "1");
				conf.put("ganglia.minutes", "1");
				conf.put("ganglia.UDPAddressingMode", "UNICAST");

				conf.put("refreshtime", "5"); // Rechazamos las cadenas que 
				conf.put("metrics.reporter.yammer.facade..metric.bucket.seconds", 10);
				//conf.put("conf.pattern2", "(<date>[^\\s]+)\\s+");
				conf.put("group.separator", "|");
//				conf.put("gangliaserver", "localhost");
//				conf.put("gangliaport", "1234");
				
				CompleteTopologyParam completeTopologyParam = new CompleteTopologyParam();
				completeTopologyParam.setMockedSources(mockedSources);
				completeTopologyParam.setStormConf(conf);
				/**
				 * TODO
				 */
				Map result = Testing.completeTopology(cluster, topology,
						completeTopologyParam);

				List tupleValues = (ArrayList)Testing.readTuples(result, "3").get(0);

				// El mensaje recibido es del tipo {"extraData":"...", "message":"..."}
				JSONParser parser = new JSONParser();
				JSONObject obj = null;
				try {
					obj = (JSONObject) parser.parse((String)tupleValues.get(0));
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				String o2 = (String)obj.get("message");
				Assert.assertTrue("Existe el elemento fecha", "hola amigo 11-03-2024 22:22 sflhsldfjs 11-11-11  11:11:11 ".equals(o2));
			}

		});
	}
	
	@Test
	public void testNoPasaFiltro() {
		

		MkClusterParam mkClusterParam = new MkClusterParam();
		mkClusterParam.setSupervisors(4);
		Config daemonConf = new Config();
		daemonConf.put(Config.STORM_LOCAL_MODE_ZMQ, false);
		mkClusterParam.setDaemonConf(daemonConf);

		/**
		 * This is a combination of <code>Testing.withLocalCluster</code> and <code>Testing.withSimulatedTime</code>.
		 */
		Testing.withSimulatedTimeLocalCluster(mkClusterParam, new TestJob() {
			@Override
			public void run(ILocalCluster cluster) {

				String ret = "{\"extraData\":{\"nombre\":\"Rodrigo\"},\"message\":\"hola amigo 11-03-2024 22:22 sflhsldfjs 11-11-11  11:11:11 \"}";
				
				// build the test topology
				TopologyBuilder builder = new TopologyBuilder();
				builder.setSpout("1", new SimpleSpout(), 3);
				builder.setBolt("3", new FilterMessageBolt(), 4).shuffleGrouping("1");

				StormTopology topology = builder.createTopology();

				// complete the topology

				// prepare the mock data
				MockedSources mockedSources = new MockedSources();
				mockedSources.addMockData("1", new Values(ret.getBytes()));

				// prepare the config
				Config conf = new Config();
				conf.setNumWorkers(2);
				conf.put("filter.bolt.allow", ""); // Aceptamos las cadenas con tres digitos seguidos
				conf.put("filter.bolt.deny", "hola"); // Rechazamos las cadenas que

				conf.put("ganglia.report", "yes");
				conf.put("ganglia.host", "localhost");
				conf.put("ganglia.port", "5555");
				conf.put("ganglia.ttl", "1");
				conf.put("ganglia.minutes", "1");
				conf.put("ganglia.UDPAddressingMode", "UNICAST");

				conf.put("refreshtime", "5"); // Rechazamos las cadenas que 
				conf.put("metrics.reporter.yammer.facade..metric.bucket.seconds", 10);
				conf.put("conf.pattern1", "(<date>[^\\s]+)\\s+");
				conf.put("group.separator", "|");
//				conf.put("gangliaserver", "localhost");
//				conf.put("gangliaport", "1234");
				
				CompleteTopologyParam completeTopologyParam = new CompleteTopologyParam();
				completeTopologyParam.setMockedSources(mockedSources);
				completeTopologyParam.setStormConf(conf);
				/**
				 * TODO
				 */
				Map result = Testing.completeTopology(cluster, topology,
						completeTopologyParam);

				Assert.assertTrue("No pasa al tercer bolt", result.size() == 1);
			}

		});
	}
	
}
