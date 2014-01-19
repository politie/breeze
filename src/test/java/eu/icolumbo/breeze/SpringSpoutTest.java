package eu.icolumbo.breeze;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;


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

	Map<String,Object> stormConf = new HashMap<>();


	@Test
	public void happyFlow() throws Exception {
		SpringSpout subject = new SpringSpout(TestBean.class, "ping()", "out");
		subject.setOutputStreamId("ether");

		stormConf.put("topology.name", "topology");
		subject.open(stormConf, contextMock, collectorMock);
		subject.nextTuple();

		verify(collectorMock).emit("ether", asList((Object) "ping"));
	}

	@Test
	public void error() throws Exception {
		stormConf.put("topology.name", "topology");

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

}
