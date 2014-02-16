package eu.icolumbo.breeze.namespace;

import backtype.storm.Config;
import org.junit.Test;

import java.util.Properties;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;


/**
 * Tests {@link TopologyStarter}.
 */
public class TopologyStarterTest extends Config {

	@Test
	public void stormConfTypes() {
		Properties source = new Properties();
		source.put(TOPOLOGY_NAME, "name");
		source.put(TOPOLOGY_DEBUG, "true");
		source.put(TOPOLOGY_WORKERS, "1");
		source.put(TOPOLOGY_RECEIVER_BUFFER_SIZE, "2048");
		source.put(SUPERVISOR_SLOTS_PORTS, "7000,\t7001");
		source.put(DRPC_SERVERS, "host1,host2, host3");
		source.put("unknown", "2");

		Config result = TopologyStarter.stormConfig(source);
		assertEquals("name", result.get(TOPOLOGY_NAME));
		assertEquals(true, result.get(TOPOLOGY_DEBUG));
		assertEquals(1, result.get(TOPOLOGY_WORKERS));
		assertEquals(2048, result.get(TOPOLOGY_RECEIVER_BUFFER_SIZE));
		assertEquals(asList("host1", "host2", "host3"), result.get(DRPC_SERVERS));
		assertEquals(asList(7000, 7001), result.get(SUPERVISOR_SLOTS_PORTS));
		assertEquals("2", result.get("unknown"));
	}

}
