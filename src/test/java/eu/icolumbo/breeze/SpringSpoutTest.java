package eu.icolumbo.breeze;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Values;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.context.ApplicationContext;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;


/**
 * Tests {@link SpringSpout}.
 * @author Jethro Bakker
 * @author Pascal S. de Kloe
 */
@RunWith(MockitoJUnitRunner.class)
public class SpringSpoutTest {

	@Mock
	private SpoutOutputCollector collectorMock;

	@Mock
	private TopologyContext contextMock;

	@Mock
	ApplicationContext applicationContextMock;

	Map<String,Object> stormConf = new HashMap<>();

	@Before
	public void setup() {
		stormConf.clear();
		stormConf.put("topology.name", "simple");
	}

	@Test
	public void happyFlow() throws Exception {
		SpringSpout subject = new SpringSpout(TestBean.class, "ping()", "out");
		subject.setOutputStreamId("ether");

		subject.open(stormConf, contextMock, collectorMock);
		subject.nextTuple();

		verify(collectorMock).emit(eq("ether"), eq(asList((Object) "ping")));
	}

	@Test
	public void happyFlowTransactions() throws Exception {
		SpringSpout subject = new SpringSpout(TestBean.class, "instance()", "out");
		subject.setOutputStreamId("ether");
		subject.setAckSignature("echo(greeting)");
		subject.setFailSignature("echo(greeting)");

		subject.setApplicationContext(applicationContextMock);
		TestBean testBeanMock = mock(TestBean.class);
		TestBean testBeanMock2 = mock(TestBean.class);
		doReturn(testBeanMock).when(applicationContextMock).getBean(TestBean.class);

		doReturn(testBeanMock2).when(testBeanMock).instance();
		doReturn("hi!").when(testBeanMock2).getGreeting();

		subject.open(stormConf, contextMock, collectorMock);
		subject.nextTuple();

		ArgumentCaptor<TransactionMessageId> transactionCaptor = ArgumentCaptor.forClass(TransactionMessageId.class);
		verify(collectorMock).emit(eq("ether"), eq(asList((Object) testBeanMock2)), transactionCaptor.capture());

		subject.ack(transactionCaptor.getValue());

		verify(testBeanMock).echo("hi!");
	}

	@Test
	public void error() throws Exception {
		SpringSpout subject = new SpringSpout(TestBean.class, "clone()", "copy");

		subject.open(stormConf, contextMock, collectorMock);
		subject.nextTuple();

		verify(collectorMock).reportError(any(CloneNotSupportedException.class));
		verifyNoMoreInteractions(collectorMock);
	}

	@Test
	public void nop() {
		SpringSpout subject = new SpringSpout(Object.class, "nop()");

		subject.close();
		subject.activate();
		subject.deactivate();
		subject.ack(null);
		subject.fail(null);
		verifyZeroInteractions(contextMock);
		verifyZeroInteractions(collectorMock);
	}

	@Test
	public void init() throws Exception {
		SpringSpout subject = new SpringSpout(TestBean.class, "ping()", "out");
		subject.setAckSignature("echo(greeting)");
		subject.setFailSignature("echo(greeting)");

		subject.open(stormConf, contextMock, collectorMock);

		Method expected = SpringSpout.findMethod(TestBean.class, "echo", 1);
		assertEquals(expected, subject.getAckMethod());
		assertEquals(expected, subject.getFailMethod());
	}

	@Test(expected = IllegalStateException.class)
	public void illegalAckSignature() throws Exception {
		SpringSpout subject = new SpringSpout(TestBean.class, "ping()", "out");
		subject.setAckSignature("failMethod(greeting)");

		subject.open(stormConf, contextMock, collectorMock);
	}

	@Test
	public void ack() throws Exception {
		SpringSpout subject = new SpringSpout(TestBean.class, "ping()", "out");
		subject.setAckSignature("echo(greeting)");

		subject.setApplicationContext(applicationContextMock);
		TestBean testBeanMock = mock(TestBean.class);
		doReturn(testBeanMock).when(applicationContextMock).getBean(TestBean.class);

		subject.open(stormConf, contextMock, collectorMock);

		TransactionMessageId messageId = new TransactionMessageId();
		messageId.setAck(new Values("1234"));
		subject.ack(messageId);

		verify(testBeanMock).echo("1234");
	}

	@Test
	public void fail() throws Exception {
		SpringSpout subject = new SpringSpout(TestBean.class, "ping()", "out");
		subject.setFailSignature("echo(greeting)");

		subject = spy(subject);

		subject.open(stormConf, contextMock, collectorMock);

		TransactionMessageId messageId = new TransactionMessageId();
		messageId.setFail(new Values("1234"));
		subject.fail(messageId);

		Method method = SpringSpout.findMethod(TestBean.class, "echo", 1);
		verify(subject).invoke(method, "1234");
	}

	@Test
	public void transactionMapping() throws Exception {
		SpringSpout subject = new SpringSpout(TestBean.class, "ping()", "out");

		TestBean testBean = new TestBean();
		testBean.setGreeting("hello");

		String[] fields = {"greeting"};
		Values values = subject.getTransactionMapping(testBean, fields);

		assertEquals("hello", values.get(0));
	}
}
