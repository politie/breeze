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
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.context.ApplicationContext;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
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


	@Before
	public void init() {
		stormConf.clear();
		stormConf.put("topology.name", "topology");
		doReturn(new TestBean()).when(applicationContextMock).getBean(TestBean.class);
	}

	private void run(SpringBolt subject) {
		subject.setApplicationContext(applicationContextMock);
		subject.prepare(stormConf, topologyContextMock, outputCollectorMock);
		subject.declareOutputFields(outputFieldsDeclarerMock);
		subject.execute(tupleMock);
	}

	@Test
	public void pipe() {
		doReturn("Hello World!").when(tupleMock).getValueByField("in");

		SpringBolt subject = new SpringBolt(TestBean.class, "echo(in)", "out");
		subject.setDoAnchor(false);
		run(subject);

		verify(outputCollectorMock).emit(asList((Object) "Hello World!"));
		verify(outputCollectorMock).ack(tupleMock);
	}

	@Test
	public void noInput() {
		run(new SpringBolt(TestBean.class, "ping()", "out"));

		verify(outputCollectorMock).emit(tupleMock, asList((Object) "ping"));
		verify(outputCollectorMock).ack(tupleMock);
	}

	@Test
	public void noOutput() {
		SpringBolt subject = new SpringBolt(TestBean.class, "nop()");
		run(subject);
		verify(outputCollectorMock).ack(tupleMock);
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
		SpringBolt subject = new SpringBolt(TestBean.class, "hashCode()", "hash");
		subject.prepare(stormConf, topologyContextMock, outputCollectorMock);
		subject.execute(tupleMock);
		subject.execute(tupleMock);
		ArgumentCaptor<List> captor = ArgumentCaptor.forClass(List.class);
		verify(outputCollectorMock, times(2)).emit(same(tupleMock), captor.capture());
		assertNotEquals(captor.getAllValues().get(0), captor.getAllValues().get(1));
	}

	@Test
	public void scatteredArray() {
		doReturn("first").when(tupleMock).getValueByField("a");
		doReturn("second").when(tupleMock).getValueByField("b");
		SpringBolt subject = new SpringBolt(TestBean.class, "array(a, b)", "x");
		subject.setScatterOutput(true);
		run(subject);

		InOrder order = inOrder(outputCollectorMock);
		order.verify(outputCollectorMock).emit(same(tupleMock), eq(asList((Object) "first")));
		order.verify(outputCollectorMock).emit(same(tupleMock), eq(asList((Object) "second")));
		order.verify(outputCollectorMock).ack(tupleMock);
		verifyNoMoreInteractions(outputCollectorMock);
	}

	@Test
	public void scatteredCollection() {
		doReturn("first").when(tupleMock).getValueByField("a");
		doReturn("second").when(tupleMock).getValueByField("b");
		SpringBolt subject = new SpringBolt(TestBean.class, "list(a, b)", "x");
		subject.setScatterOutput(true);
		run(subject);

		InOrder order = inOrder(outputCollectorMock);
		order.verify(outputCollectorMock).emit(same(tupleMock), eq(asList((Object) "first")));
		order.verify(outputCollectorMock).emit(same(tupleMock), eq(asList((Object) "second")));
		order.verify(outputCollectorMock).ack(tupleMock);
		verifyNoMoreInteractions(outputCollectorMock);
	}

	@Test
	public void scatteredNull() {
		SpringBolt subject = new SpringBolt(TestBean.class, "none()", "y");
		subject.setScatterOutput(true);
		run(subject);

		verify(outputCollectorMock).ack(tupleMock);
		verifyNoMoreInteractions(outputCollectorMock);
	}

	@Test
	public void unscatterable() {
		SpringBolt subject = new SpringBolt(TestBean.class, "ping()", "y");
		subject.setScatterOutput(true);
		run(subject);

		verify(outputCollectorMock).emit(same(tupleMock), eq(asList((Object) "ping")));
		verify(outputCollectorMock).ack(tupleMock);
		verifyNoMoreInteractions(outputCollectorMock);
	}

	@Test
	public void singleOutputNull() {
		run(new SpringBolt(TestBean.class, "none()", "y"));

		verify(outputCollectorMock).emit(same(tupleMock), eq(singletonList(null)));
		verify(outputCollectorMock).ack(tupleMock);
		verifyNoMoreInteractions(outputCollectorMock);
	}

	@Test
	public void singleOutputCollection() {
		doReturn("first").when(tupleMock).getValueByField("a");
		doReturn("second").when(tupleMock).getValueByField("b");
		run(new SpringBolt(TestBean.class, "list(a, b)", "y"));

		verify(outputCollectorMock).emit(same(tupleMock), eq(asList((Object) asList("first", "second"))));
		verify(outputCollectorMock).ack(tupleMock);
		verifyNoMoreInteractions(outputCollectorMock);
	}

	@Test
	public void singleOutputPassThrough() {
		doReturn("ear").when(tupleMock).getValueByField("sensor");
		SpringBolt subject = new SpringBolt(TestBean.class, "ping()", "ack");
		subject.setPassThroughFields("timer", "sensor");
		run(subject);

		ArgumentCaptor<Fields> fieldsCaptor = ArgumentCaptor.forClass(Fields.class);
		verify(outputFieldsDeclarerMock).declare(fieldsCaptor.capture());
		assertEquals(asList("ack", "timer", "sensor"), fieldsCaptor.getValue().toList());

		verify(outputCollectorMock).emit(same(tupleMock), eq(asList((Object) "ping", null, "ear")));
		verify(outputCollectorMock).ack(tupleMock);
		verifyNoMoreInteractions(outputCollectorMock);
	}

	@Test
	public void multipleOutput() {
		doReturn("check").when(tupleMock).getValueByField("a");
		doReturn("double").when(tupleMock).getValueByField("b");
		run(new SpringBolt(TestBean.class, "map(a, b)", "x", "y", "z"));

		verify(outputCollectorMock).emit(same(tupleMock), eq(asList((Object) "check", "double", null)));
		verify(outputCollectorMock).ack(tupleMock);
		verifyNoMoreInteractions(outputCollectorMock);
	}

	@Test
	public void multipleOutputPassThrough() {
		doReturn("check").when(tupleMock).getValueByField("a");
		doReturn("double").when(tupleMock).getValueByField("b");
		doReturn("again").when(tupleMock).getValueByField("c");
		SpringBolt subject = new SpringBolt(TestBean.class, "map(a, b)", "x", "y");
		subject.setPassThroughFields("c");
		run(subject);

		ArgumentCaptor<Fields> fieldsCaptor = ArgumentCaptor.forClass(Fields.class);
		verify(outputFieldsDeclarerMock).declare(fieldsCaptor.capture());
		assertEquals(asList("x", "y", "c"), fieldsCaptor.getValue().toList());

		verify(outputCollectorMock).emit(same(tupleMock), eq(asList((Object) "check", "double", "again")));
		verify(outputCollectorMock).ack(tupleMock);
		verifyNoMoreInteractions(outputCollectorMock);
	}

	@Test
	public void multipleOutputNull() {
		run(new SpringBolt(TestBean.class, "none()", "x", "y", "z"));

		verify(outputCollectorMock).emit(same(tupleMock), eq(asList(null, null, null)));
		verify(outputCollectorMock).ack(tupleMock);
		verifyNoMoreInteractions(outputCollectorMock);
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void passThroughOverlap() {
		SpringBolt subject = new SpringBolt(Object.class, "hashCode()", "hash");
		subject.setPassThroughFields("dupe", "hash");
	}

}
