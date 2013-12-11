package eu.icolumbo.breeze.namespace;

import backtype.storm.generated.Bolt;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.Grouping;
import backtype.storm.generated.SpoutSpec;
import backtype.storm.generated.StormTopology;
import eu.icolumbo.breeze.SpringBolt;
import eu.icolumbo.breeze.SpringSpout;
import org.junit.Test;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.context.support.AbstractXmlApplicationContext;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;

import java.nio.charset.Charset;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;


/**
 * Integration tests for XML definitions.
 * @author Pascal S. de Kloe
 */
public class BeanDefinitionTest extends AbstractXmlApplicationContext {

	private static final Charset UTF8 = Charset.forName("US-ASCII");
	private String beansXml;


	@Test
	public void build() throws Exception {
		beansXml = "<breeze:topology id='testTopology'>\n" +
				"<breeze:spout id='testSpout' beanType='eu.icolumbo.breeze.TestBean' signature='ping()' outputFields='feed'/>\n" +
				"<breeze:bolt id='testBolt' beanType='eu.icolumbo.breeze.TestBean' signature='echo(feed)' parallelism='2'/>\n" +
				"</breeze:topology>\n";
		refresh();

		StormTopology topology = getBean(StormTopology.class);
		assertSame("topology by ID", topology, getBean("testTopology"));
		assertEquals("spout count", 1, topology.get_spouts_size());
		assertEquals("bolt count", 1, topology.get_bolts_size());

		SpringSpout spout = getBean("testSpout", SpringSpout.class);
		assertNotNull("spout by ID", spout);
		SpoutSpec spoutSpec = topology.get_spouts().get("testSpout");
		assertNotNull("spout spec by ID", spoutSpec);

		SpringBolt bolt = getBean("testBolt", SpringBolt.class);
		assertNotNull("bolt by ID", bolt);
		Bolt boltSpec = topology.get_bolts().get("testBolt");
		assertNotNull("bolt spec by ID", boltSpec);

		assertEquals("spout parralelism", 0, spoutSpec.get_common().get_parallelism_hint());
		assertEquals("bolt parralelism", 2, boltSpec.get_common().get_parallelism_hint());

		Map<GlobalStreamId,Grouping> boltInputs = boltSpec.get_common().get_inputs();
		assertEquals("input size", 1, boltInputs.size());
		GlobalStreamId streamId = boltInputs.keySet().iterator().next();
		assertEquals("input component id", "testSpout", streamId.get_componentId());
		assertEquals("input stream id", "default", streamId.get_streamId());
	}

	@Test
	public void brokenWithUnboundBolt() throws Exception {
		beansXml = "<breeze:topology id='testTopology'>\n" +
				"<breeze:spout beanType='eu.icolumbo.breeze.TestBean' signature='ping()' outputFields='feed'/>\n" +
				"<breeze:bolt id='unbound' beanType='eu.icolumbo.breeze.TestBean' signature='echo(other)'/>\n" +
				"</breeze:topology>\n";
		refresh();

		try {
			getBean(StormTopology.class);
			fail("no exception");
		} catch (BeanCreationException e) {
			Throwable cause = e.getCause();
			assertNotNull("cause", cause);
			assertEquals("Can't resolve all input fields for: [SpringBolt 'unbound']", cause.getMessage());
		}
	}

	@Override
	public Resource[] getConfigResources() {
		String xml = "<?xml version='1.0' encoding='US-ASCII'?>\n" +
				"<beans xmlns='http://www.springframework.org/schema/beans'\n" +
				"  xmlns:breeze='http://www.icolumbo.eu/2013/breeze'\n" +
				"  xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance'\n" +
				"  xsi:schemaLocation='http://www.springframework.org/schema/beans\n" +
				"    http://www.springframework.org/schema/beans/spring-beans.xsd\n" +
				"    http://www.icolumbo.eu/2013/breeze\n" +
				"    http://www.icolumbo.eu/2013/breeze-1.0.xsd'>\n\n";
		xml += beansXml;
		xml += "</beans>";
		return new Resource[] {new ByteArrayResource(xml.getBytes(UTF8))};
	}

}
