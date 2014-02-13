package eu.icolumbo.breeze.namespace;

import backtype.storm.Config;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertEquals;


/**
 * Tests {@link TopologyStarter}.
 */
public class TopologyStarterTest extends Config {

	@Test
	public void stormConfTypes() {
		Properties source = new Properties();
		source.put(Config.TOPOLOGY_NAME, "name");
		source.put(Config.TOPOLOGY_DEBUG, "true");
		source.put(Config.TOPOLOGY_WORKERS, "1");
		source.put("unknown", "2");

		Config result = TopologyStarter.stormConfig(source);
		assertEquals("name", result.get(Config.TOPOLOGY_NAME));
		assertEquals(true, result.get(Config.TOPOLOGY_DEBUG));
		assertEquals(1, result.get(Config.TOPOLOGY_WORKERS));
		assertEquals("2", result.get("unknown"));
	}

}
