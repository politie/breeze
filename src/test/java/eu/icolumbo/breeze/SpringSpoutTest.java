package eu.icolumbo.breeze;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Values;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import java.util.Collections;
import java.util.Map;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
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

	Map<String,Object> stormConf = Collections.EMPTY_MAP;


	@Test
	public void happyFlow() throws Exception {
		doReturn("topology").when(contextMock).getStormId();

		SpringSpout subject = new SpringSpout(TestBean.class, "ping()", "echo");
		subject.open(stormConf, contextMock, collectorMock);
		subject.nextTuple();

		verify(collectorMock).emit(new Values("ping"));
	}

	@Test
	public void error() throws Exception {
		doReturn("topology").when(contextMock).getStormId();

		SpringSpout subject = new SpringSpout(TestBean.class, "clone()", "copy");
		subject.open(stormConf, contextMock, collectorMock);
		subject.nextTuple();

		verify(collectorMock).reportError(any(CloneNotSupportedException.class));
		verify(collectorMock, never()).emit(anyList());
	}

	@Test
	public void nop() {
		SpringSpout subject = new SpringSpout(Object.class, "ping()", "echo");
		reset(collectorMock, contextMock);

		subject.close();
		subject.activate();
		subject.deactivate();
		subject.ack(null);
		subject.fail(null);
		verifyZeroInteractions(contextMock);
		verifyZeroInteractions(collectorMock);
	}

}
