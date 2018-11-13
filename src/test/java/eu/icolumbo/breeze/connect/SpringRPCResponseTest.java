package eu.icolumbo.breeze.connect;

import org.apache.storm.Config;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.Collections;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;


/**
 * Tests {@link SpringRPCResponse}.
 * @author Pascal S. de Kloe
 */
public class SpringRPCResponseTest {

	@Test
	public void identification() {
		SpringRPCResponse subject = new SpringRPCResponse("fn(x)", "y");
		assertEquals("fn-rpc-rsp", subject.getId());
		assertEquals("SpringRPCResponse 'fn'", subject.toString());
	}

	@Test
	public void passThrough() {
		SpringRPCResponse subject = new SpringRPCResponse("fn(x)", "y");
		subject.setPassThroughFields("a", "b");
		assertArrayEquals(new String[]{"y", "fn-rpc-ctx"}, subject.getInputFields());

		OutputFieldsDeclarer declarerMock = mock(OutputFieldsDeclarer.class);
		subject.declareOutputFields(declarerMock);
		ArgumentCaptor<Fields> fieldsCaptor = ArgumentCaptor.forClass(Fields.class);
		verify(declarerMock).declareStream(eq("default"), fieldsCaptor.capture());
		assertEquals(asList("a", "b"), fieldsCaptor.getValue().toList());
	}

	@Test
	public void output() {
		SpringRPCResponse subject = new SpringRPCResponse("fn(x)", "y");
		subject.setPassThroughFields("a", "b");

		Map conf = Collections.singletonMap(Config.STORM_CLUSTER_MODE, "arbitrary");
		OutputCollector collectorMock = mock(OutputCollector.class);
		subject.prepare(conf, null, collectorMock);

		Tuple tupleMock = mock(Tuple.class);
		doReturn(1).when(tupleMock).getValueByField("a");
		doReturn(2).when(tupleMock).getValueByField("b");

		subject.execute(tupleMock);
		verify(collectorMock).emit("default", tupleMock, asList((Object) 1, 2));

		subject.setDoAnchor(false);
		subject.execute(tupleMock);
		verify(collectorMock).emit("default", asList((Object) 1, 2));
	}

	@Test
	public void outputFields() {
		SpringRPCResponse subject = new SpringRPCResponse("fn(x)", "y");
		assertEquals("count", 0, subject.getOutputFields().length);
	}

	@Test
	public void parallelism() {
		SpringRPCResponse subject = new SpringRPCResponse("fn(x)", "y");
		assertNull("default", subject.getParallelism());
		subject.setParallelism(3);
		assertEquals(3, subject.getParallelism());
	}

	@Test
	public void noOutputField() {
		try {
			new SpringRPCResponse("fn(x)");
			fail("no exception");
		} catch (UnsupportedOperationException e) {
			String expected = "Breeze RPC requires 1 output field for now";
			assertEquals(expected, e.getMessage());
		}
	}

}
