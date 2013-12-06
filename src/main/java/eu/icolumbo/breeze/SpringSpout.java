package eu.icolumbo.breeze;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;

import static java.lang.String.format;


/**
 * Spring for Storm spouts.
 * @author Jethro Bakker
 * @author Pascal S. de Kloe
 */
public class SpringSpout extends SpringComponent implements IRichSpout {

	private static final Logger logger = LoggerFactory.getLogger(SpringSpout.class);

	private SpoutOutputCollector collector;


	public SpringSpout(Class<?> beanType, String invocation, String... outputFields) {
		super(beanType, invocation, outputFields);
		logger.trace("{} constructed", this);
	}

	@Override
	public void open(Map stormConf, TopologyContext topologyContext, SpoutOutputCollector outputCollector) {
		collector = outputCollector;
		super.init(stormConf, topologyContext);
	}

	@Override
	public void nextTuple() {
		try {
			Values[] entries = invoke();
			logger.debug(format("%s got %d tuples", this, entries.length));
			for (Values output : entries)
				collector.emit(output);
		} catch (InvocationTargetException e) {
			collector.reportError(e.getCause());
		}
	}

	@Override
	public void close() {
	}

	@Override
	public void activate() {
	}

	@Override
	public void deactivate() {
	}

	@Override
	public void ack(Object o) {
	}

	@Override
	public void fail(Object o) {
	}

}
