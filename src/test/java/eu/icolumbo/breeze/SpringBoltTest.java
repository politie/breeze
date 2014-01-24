package eu.icolumbo.breeze;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.context.ApplicationContext;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;


/**
 * Tests {@link SpringBolt}.
 * @author Pascal S. de Kloe
 * @author Jethro Bakker
 */
@RunWith(MockitoJUnitRunner.class)
public class SpringBoltTest {

	@Mock
	Tuple tupleMock;

	@Mock
	OutputCollector outputCollectorMock;

	@Mock
	TopologyContext topologyContextMock;

	@Mock
	OutputFieldsDeclarer outputFieldsDeclarerMock;

	@Mock
	ApplicationContext applicationContextMock;

	Map<String,Object> stormConf = new HashMap();

	@Captor
	ArgumentCaptor<Fields> outputFieldsCaptor;


	@Before
	public void init() {
		stormConf.clear();
		stormConf.put("topology.name", "topology");
		doReturn(new TestBean()).when(applicationContextMock).getBean(TestBean.class);
	}

	private void run(SpringBolt subject) {
		run(subject, null);
	}

	private void run(SpringBolt subject, String stream) {
		if (stream == null)
			stream = "default";
		else
			subject.setOutputStreamId(stream);

		subject.setApplicationContext(applicationContextMock);
		subject.prepare(stormConf, topologyContextMock, outputCollectorMock);

		subject.declareOutputFields(outputFieldsDeclarerMock);
		verify(outputFieldsDeclarerMock).declareStream(eq(stream), outputFieldsCaptor.capture());

		subject.execute(tupleMock);
	}

	@Test
	public void pipe() {
		List<Object> expected = asList((Object) "Hello World!");
		doReturn(expected.get(0)).when(tupleMock).getValueByField("in");
		SpringBolt subject = new SpringBolt(TestBean.class, "echo(in)", "out");

		run(subject);
		assertEquals(asList("out"), outputFieldsCaptor.getValue().toList());
		verify(outputCollectorMock).emit("default", tupleMock, expected);
		verify(outputCollectorMock).ack(tupleMock);
		verifyNoMoreInteractions(outputCollectorMock);
	}

	@Test
	public void drainUnanchoredCustomStream() {
		List<String> expectedValues = asList("Hello", "World");
		List<Object> expected = asList((Object) expectedValues);
		doReturn(expectedValues.get(0)).when(tupleMock).getValueByField("a");
		doReturn(expectedValues.get(1)).when(tupleMock).getValueByField("b");
		SpringBolt subject = new SpringBolt(TestBean.class, "list(a, b)", "y");
		subject.setDoAnchor(false);

		run(subject, "drain");
		assertEquals(asList("y"), outputFieldsCaptor.getValue().toList());
		verify(outputCollectorMock).emit("drain", expected);
		verify(outputCollectorMock).ack(tupleMock);
		verifyNoMoreInteractions(outputCollectorMock);
	}

	@Test
	public void noInputPassThrough() {
		List<Object> expected = asList((Object) "ping", "pong");
		doReturn(expected.get(1)).when(tupleMock).getValueByField("ack");
		SpringBolt subject = new SpringBolt(TestBean.class, "ping()", "out");
		subject.setPassThroughFields("ack");

		run(subject);
		assertEquals(asList("out", "ack"), outputFieldsCaptor.getValue().toList());
		verify(outputCollectorMock).emit("default", tupleMock, expected);
		verify(outputCollectorMock).ack(tupleMock);
		verifyNoMoreInteractions(outputCollectorMock);
	}

	@Test
	public void noOutputPassThrough() {
		List<Object> expected = asList((Object) 4);
		doReturn(expected.get(0)).when(tupleMock).getValueByField("n");
		SpringBolt subject = new SpringBolt(TestBean.class, "nop()");
		subject.setPassThroughFields("n");

		run(subject);
		assertEquals(asList("n"), outputFieldsCaptor.getValue().toList());
		verify(outputCollectorMock).emit("default", tupleMock, expected);
		verify(outputCollectorMock).ack(tupleMock);
		verifyNoMoreInteractions(outputCollectorMock);
	}

	@Test
	public void noIO() {
		SpringBolt subject = new SpringBolt(TestBean.class, "nop()");

		run(subject);
		assertEquals(asList(), outputFieldsCaptor.getValue().toList());
		verify(outputCollectorMock).ack(tupleMock);
		verifyNoMoreInteractions(outputCollectorMock);
	}

	@Test
	public void nullObjectProperties() {
		List<Object> expected = asList(null, null, null);
		SpringBolt subject = new SpringBolt(TestBean.class, "nullObject()", "x", "y", "z");

		run(subject, "complex");
		verify(outputCollectorMock).emit("complex", tupleMock, expected);
		verify(outputCollectorMock).ack(tupleMock);
		verifyNoMoreInteractions(outputCollectorMock);
	}

	@Test
	public void multipleOutputPassThrough() {
		List<Object> expected = asList((Object) "check", "double", "again");
		doReturn(expected.get(0)).when(tupleMock).getValueByField("a");
		doReturn(expected.get(1)).when(tupleMock).getValueByField("b");
		doReturn(expected.get(2)).when(tupleMock).getValueByField("c");
		SpringBolt subject = new SpringBolt(TestBean.class, "map(a, b)", "x", "y");
		subject.setPassThroughFields("c");

		run(subject);
		assertEquals(asList("x", "y", "c"), outputFieldsCaptor.getValue().toList());
		verify(outputCollectorMock).emit("default", tupleMock, expected);
		verify(outputCollectorMock).ack(tupleMock);
		verifyNoMoreInteractions(outputCollectorMock);
	}

	@Test
	public void scatteredArray() {
		List<Object> expectedFirst = asList((Object) "first");
		List<Object> expectedSecond = asList((Object) "second");
		doReturn(expectedFirst.get(0)).when(tupleMock).getValueByField("a");
		doReturn(expectedSecond.get(0)).when(tupleMock).getValueByField("b");
		SpringBolt subject = new SpringBolt(TestBean.class, "array(a, b)", "x");
		subject.setScatterOutput(true);

		run(subject);
		InOrder order = inOrder(outputCollectorMock);
		order.verify(outputCollectorMock).emit("default", tupleMock, expectedFirst);
		order.verify(outputCollectorMock).emit("default", tupleMock, expectedSecond);
		order.verify(outputCollectorMock).ack(tupleMock);
		verifyNoMoreInteractions(outputCollectorMock);
	}

	@Test
	public void scatteredMultipleOutputCollection() {
		Map<?,?> first = singletonMap("x", 1);
		TestBean.Data second = new TestBean.Data();
		second.setMessage("Hello");

		doReturn(first).when(tupleMock).getValueByField("a");
		doReturn(second).when(tupleMock).getValueByField("b");
		SpringBolt subject = new SpringBolt(TestBean.class, "list(a, b)", "x", "message");
		subject.setScatterOutput(true);
		subject.setDoAnchor(false);

		run(subject, "collect");
		InOrder order = inOrder(outputCollectorMock);
		order.verify(outputCollectorMock).emit("collect", asList((Object) 1, null));
		order.verify(outputCollectorMock).emit("collect", asList((Object) null, "Hello"));
		order.verify(outputCollectorMock).ack(tupleMock);
		verifyNoMoreInteractions(outputCollectorMock);
	}

	@Test
	public void scatterPass() {
		List<Object> expected = asList((Object) "ping");
		SpringBolt subject = new SpringBolt(TestBean.class, "ping()", "y");
		subject.setScatterOutput(true);

		run(subject);
		verify(outputCollectorMock).emit("default", tupleMock, expected);
		verify(outputCollectorMock).ack(tupleMock);
		verifyNoMoreInteractions(outputCollectorMock);
	}

	@Test
	public void scatterFilter() {
		SpringBolt subject = new SpringBolt(TestBean.class, "nullObject()", "y");
		subject.setScatterOutput(true);

		run(subject, "filtered");
		verify(outputCollectorMock).ack(tupleMock);
		verifyNoMoreInteractions(outputCollectorMock);
	}

	@Test
	public void scatterNull() {
		SpringBolt subject = new SpringBolt(TestBean.class, "nullArray()", "y", "z");
		subject.setScatterOutput(true);

		run(subject);
		verify(outputCollectorMock).ack(tupleMock);
		verifyNoMoreInteractions(outputCollectorMock);
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void passThroughOverlap() {
		SpringBolt subject = new SpringBolt(Object.class, "hashCode()", "hash");
		subject.setPassThroughFields("dupe", "hash");
	}

	@Test
	public void error() {
		SpringBolt subject = new SpringBolt(TestBean.class, "clone()");
		run(subject);
		verify(outputCollectorMock, never()).ack(tupleMock);
		verify(outputCollectorMock).fail(tupleMock);
	}

	@Test
	public void frameworkError() {
		RuntimeException cause = new RuntimeException("test");
		doThrow(cause).when(tupleMock).getValueByField("oops");

		SpringBolt subject = new SpringBolt(TestBean.class, "echo(oops)");
		try {
			run(subject);
			fail("no exception");
		} catch (RuntimeException e) {
			assertSame(cause, e);
		}

		verifyZeroInteractions(outputCollectorMock);
	}

	@Test
	public void cleanup() {
		new SpringBolt(TestBean.class, "nop()").cleanup();
	}

	@Test
	public void prototypeIntegrationRun() {
		stormConf.put("topology.name", "prototyping");
		SpringBolt subject = new SpringBolt(TestBean.class, "hashCode()", "hash");
		subject.prepare(stormConf, topologyContextMock, outputCollectorMock);
		subject.execute(tupleMock);
		subject.execute(tupleMock);
		ArgumentCaptor<List> captor = ArgumentCaptor.forClass(List.class);
		verify(outputCollectorMock, times(2)).emit(eq("default"),
				same(tupleMock), captor.capture());
		assertNotEquals(captor.getAllValues().get(0), captor.getAllValues().get(1));
	}

}
