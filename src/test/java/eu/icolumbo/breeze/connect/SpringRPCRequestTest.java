package eu.icolumbo.breeze.connect;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;


/**
 * Tests {@link SpringRPCRequest}.
 * @author Pascal S. de Kloe
 */
public class SpringRPCRequestTest {

	@Test
	public void identification() {
		SpringRPCRequest subject = new SpringRPCRequest("fn(x)");
		assertEquals("fn-rpc-req", subject.getId());
		assertEquals("SpringRPCRequest 'fn'", subject.toString());
	}

	@Test
	public void oneArgument() {
		SpringRPCRequest subject = new SpringRPCRequest("fn(x)");
		OutputFieldsDeclarer declarerMock = mock(OutputFieldsDeclarer.class);
		subject.declareOutputFields(declarerMock);
		ArgumentCaptor<Fields> fieldsCaptor = ArgumentCaptor.forClass(Fields.class);
		verify(declarerMock).declareStream(eq("default"), fieldsCaptor.capture());
		assertEquals(asList("x", "fn-rpc-ctx"), fieldsCaptor.getValue().toList());
	}

	@Test
	public void noArguments() {
		SpringRPCRequest subject = new SpringRPCRequest("fn()");
		OutputFieldsDeclarer declarerMock = mock(OutputFieldsDeclarer.class);
		subject.declareOutputFields(declarerMock);
		ArgumentCaptor<Fields> fieldsCaptor = ArgumentCaptor.forClass(Fields.class);
		verify(declarerMock).declareStream(eq("default"), fieldsCaptor.capture());
		assertEquals(asList("_ignoreArguments", "fn-rpc-ctx"), fieldsCaptor.getValue().toList());
	}

	@Test
	public void parallelism() {
		SpringRPCRequest subject = new SpringRPCRequest("fn(x)");
		assertNull("default", subject.getParallelism());
		subject.setParallelism(3);
		assertEquals(3, subject.getParallelism());
	}

	@Test
	public void multipleArguments() {
		try {
			new SpringRPCRequest("fn(a, b)");
			fail("no exception");
		} catch (UnsupportedOperationException e) {
			String expected = "Breeze RPC supports only one argument for now";
			assertEquals(expected, e.getMessage());
		}
	}

}
