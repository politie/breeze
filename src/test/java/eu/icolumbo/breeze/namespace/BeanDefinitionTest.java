package eu.icolumbo.breeze.namespace;

import eu.icolumbo.breeze.FunctionSignature;
import eu.icolumbo.breeze.SpringBolt;
import eu.icolumbo.breeze.SpringComponent;
import eu.icolumbo.breeze.SpringSpout;

import org.apache.storm.generated.Bolt;
import org.apache.storm.generated.ComponentCommon;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.generated.Grouping;
import org.apache.storm.generated.SpoutSpec;
import org.apache.storm.generated.StormTopology;
import org.junit.Test;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.BeanDefinitionStoreException;
import org.springframework.context.support.AbstractXmlApplicationContext;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;
import org.springframework.expression.Expression;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.Charset;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;


/**
 * Integration tests for XML definitions.
 * @author Pascal S. de Kloe
 * @author Jethro Bakker
 */
public class BeanDefinitionTest extends AbstractXmlApplicationContext {

	private static final Charset UTF8 = Charset.forName("US-ASCII");

	private String beansXml;


	@Test
	public void build() throws Exception {
		beansXml = "<breeze:topology id='t1'>" +
				"<breeze:spout id='s1' beanType='eu.icolumbo.breeze.TestBean' signature='ping()' outputFields='feed'/>" +
				"<breeze:bolt id='b1' beanType='eu.icolumbo.breeze.TestBean' signature='echo(feed)' outputFields='replay' scatterOutput='true'/>" +
				"<breeze:bolt beanType='eu.icolumbo.breeze.TestBean' signature='drain(replay)' parallelism='2'/>" +
				"</breeze:topology>";
		refresh();

		StormTopology topology = getBean("t1", StormTopology.class);
		assertEquals("spout count", 1, topology.get_spouts_size());
		assertEquals("bolt count", 2, topology.get_bolts_size());

		SpringSpout spout = getBean("s1", SpringSpout.class);
		assertEquals("spout ID", "s1", spout.getId());
		assertEquals("spout scatter", false, spout.getScatterOutput());
		SpringBolt bolt = getBean("b1", SpringBolt.class);
		assertEquals("bolt ID", "b1", bolt.getId());
		assertEquals("bolt scatter", true, bolt.getScatterOutput());

		Map<String, SpoutSpec> topologySpouts = topology.get_spouts();
		SpoutSpec spoutSpec = topologySpouts.get("s1");
		assertNotNull("s1 spec", spoutSpec);

		Map<String, Bolt> topologyBolts = topology.get_bolts();
		Bolt boltSpec = topologyBolts.get("b1");
		assertNotNull("b1 spec", boltSpec);

		String anonymousBoltId = null;
		for (String id : topologyBolts.keySet())
			if (! "b1".equals(id))
				anonymousBoltId = id;
		assertNotNull("anonymous ID", anonymousBoltId);
		Bolt anonymousBoltSpec = topologyBolts.get(anonymousBoltId);
		assertNotNull("anonymous spec", anonymousBoltSpec);

		assertEquals("s1 parralelism", 1, spoutSpec.get_common().get_parallelism_hint());
		assertEquals("b1 parralelism", 1, boltSpec.get_common().get_parallelism_hint());
		assertEquals("second bold parrallelism", 2, anonymousBoltSpec.get_common().get_parallelism_hint());

		Map<GlobalStreamId,Grouping> boltInputs = boltSpec.get_common().get_inputs();
		assertEquals("input size", 1, boltInputs.size());
		GlobalStreamId streamId = boltInputs.keySet().iterator().next();
		assertEquals("input component id", "s1", streamId.get_componentId());
		assertEquals("input stream id", "default", streamId.get_streamId());
	}


	@Test
	public void aggregate() throws Exception {
		beansXml = "<breeze:topology id='aggregate'>" +
				"<breeze:spout id='iterator' beanType='java.util.Iterator' signature='next()' outputFields='x'/>" +
				"<breeze:spout id='queue' beanType='java.util.Queue' signature='poll()' outputFields='x'/>" +
				"<breeze:bolt id='collector' beanType='org.slf4j.Logger' signature='info(x)'/>" +
				"</breeze:topology>";
		refresh();

		StormTopology topology = getBean("aggregate", StormTopology.class);

		Bolt collector = topology.get_bolts().get("collector");
		Map<GlobalStreamId, Grouping> inputs = collector.get_common().get_inputs();
		assertEquals("input count", 2, inputs.size());
		assertNotNull("iterator grouping", inputs.get(new GlobalStreamId("iterator", "default")));
		assertNotNull("queue grouping", inputs.get(new GlobalStreamId("queue", "default")));
	}

	@Test
	public void rpc() throws Exception {
		beansXml = "<breeze:topology id='t1'>" +
				"<breeze:rpc signature='fn(feed)' outputFields='result'/>" +
				"<breeze:bolt id='b1' beanType='eu.icolumbo.breeze.TestBean' signature='echo(feed)' outputFields='replay'/>" +
				"<breeze:bolt id='b2' beanType='eu.icolumbo.breeze.TestBean' signature='echo(replay)' outputFields='result'/>" +
				"</breeze:topology>";
		refresh();

		StormTopology topology = getBean("t1", StormTopology.class);
		assertEquals("spout count", 1, topology.get_spouts_size());
		assertEquals("bolt count", 3, topology.get_bolts_size());
	}

	@Test
	public void brokenWithUnboundBolt() throws Exception {
		beansXml = "<breeze:topology id='t1'>" +
				"<breeze:spout id='s1' beanType='eu.icolumbo.breeze.TestBean' signature='ping()' outputFields='feed'/>" +
				"<breeze:bolt id='b1' beanType='eu.icolumbo.breeze.TestBean' signature='echo(other)'/>" +
				"</breeze:topology>";
		refresh();

		try {
			getBean(StormTopology.class);
			fail("no exception");
		} catch (BeanCreationException e) {
			Throwable cause = e.getCause();
			assertNotNull("cause", cause);
			String expected = "Can't resolve all input fields for: [[bolt 'b1']]";
			assertEquals(expected, cause.getMessage());
		}
	}

	@Test
	public void bindings() throws Exception {
		beansXml = "<breeze:topology id='t1'>" +
				"<breeze:spout id='s1' beanType='eu.icolumbo.breeze.TestBean' signature='ping()' outputFields='feed'>" +
				"  <breeze:field name='feed' expression='#root.length()'/>" +
				"  <breeze:exception type='java.io.IOException' delay='2000'/>" +
				"</breeze:spout>" +
				"</breeze:topology>";
		refresh();

		SpringSpout spout = getBean(SpringSpout.class);
		Map<String,Expression> outputBinding = read(spout, SpringComponent.class, "outputBindingDefinitions");
		assertEquals("#root.length()", outputBinding.get("feed"));

		Map<Class<?>,Long> delayExceptions = read(spout, spout.getClass(), "delayExceptions");
		assertEquals(new Long(2000), delayExceptions.get(IOException.class));
	}

	@Test
	public void transactionAck() throws Exception {
		beansXml = "<breeze:topology id='t1'>" +
				"<breeze:spout id='s1' beanType='eu.icolumbo.breeze.TestBean' signature='ping()' outputFields='feed'>" +
				"  <breeze:transaction ack='ok()'/>" +
				"</breeze:spout>" +
				"</breeze:topology>";
		refresh();

		SpringSpout spout = getBean(SpringSpout.class);

		FunctionSignature ackSignature = read(spout, spout.getClass(), "ackSignature");
		assertEquals("ok", ackSignature.getFunction());
	}

	@Test
	public void transactionFail() throws Exception {
		beansXml = "<breeze:topology id='t1'>" +
				"<breeze:spout id='s1' beanType='eu.icolumbo.breeze.TestBean' signature='ping()' outputFields='feed'>" +
				"  <breeze:transaction fail='retry()'/>" +
				"</breeze:spout>" +
				"</breeze:topology>";
		refresh();

		SpringSpout spout = getBean(SpringSpout.class);

		FunctionSignature failSignature = read(spout, spout.getClass(), "failSignature");
		assertEquals("retry", failSignature.getFunction());
	}

	private static <T> T read(Object source, Class c, String field) throws Exception {
		Field f = c.getDeclaredField(field);
		f.setAccessible(true);
		return (T) f.get(source);
	}

	@Test
	public void unknownExceptionClass() throws Exception {
		beansXml = "<breeze:topology id='t1'>" +
				"<breeze:spout id='s1' beanType='eu.icolumbo.breeze.TestBean' signature='ping()' outputFields='feed'>" +
				"  <breeze:exception type='com.example.DoesNotExist' delay='2000'/>" +
				"</breeze:spout>" +
				"</breeze:topology>";

		try {
			refresh();
			fail("no exception");
		} catch (BeanDefinitionStoreException e) {
			Throwable cause = e.getCause();
			assertNotNull("cause", cause);
			assertEquals("No such class: com.example.DoesNotExist", cause.getMessage());
		}
	}

	@Override
	public Resource[] getConfigResources() {
		String xml = "<?xml version='1.0' encoding='US-ASCII'?>" +
				"<beans xmlns='http://www.springframework.org/schema/beans'" +
				" xmlns:breeze='http://www.icolumbo.eu/2013/breeze'" +
				" xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance'" +
				" xsi:schemaLocation='" +
				"  http://www.springframework.org/schema/beans" +
				"  http://www.springframework.org/schema/beans/spring-beans.xsd" +
				"  http://www.icolumbo.eu/2013/breeze" +
				"  http://www.icolumbo.eu/2013/breeze.xsd" +
				" '>";
		xml += beansXml;
		xml += "</beans>";
		return new Resource[] {new ByteArrayResource(xml.getBytes(UTF8))};
	}

}
