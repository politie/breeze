package eu.icolumbo.breeze;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;


/**
 * Spring for Storm spouts.
 * @author Jethro Bakker
 * @author Pascal S. de Kloe
 */
public class SpringSpout extends SpringComponent implements ConfiguredSpout {

	private static final Logger logger = LoggerFactory.getLogger(SpringSpout.class);
	private static final long serialVersionUID = 8;

	private SpoutOutputCollector collector;

	private final Map<Class<? extends Exception>,Long> delayExceptions = new HashMap<>();
	private FunctionSignature ackSignature, failSignature;
	private transient Method ackMethod, failMethod;


	public SpringSpout(Class<?> beanType, String invocation, String... outputFields) {
		super(beanType, invocation, outputFields);
	}

	@Override
	public void open(Map stormConf, TopologyContext topologyContext, SpoutOutputCollector outputCollector) {
		logger.trace("{} Storm init", this);
		collector = outputCollector;
		super.init(stormConf, topologyContext);

		try {
			if (ackSignature != null) {
				ackMethod = ackSignature.findMethod(beanType);
				logger.info("{} uses {} for transaction acknowledgement",
						this, ackMethod.toGenericString());
			}
			if (failSignature != null) {
				failMethod = failSignature.findMethod(beanType);
				logger.info("{} uses {} for transaction failures",
						this, failMethod.toGenericString());
			}
		} catch (ReflectiveOperationException e) {
			throw new IllegalStateException("Unusable transaction signature", e);
		}
	}

	@Override
	public void nextTuple() {
		logger.trace("{} next", this);
		try {
			Object[] results = invoke(EMPTY_ARRAY);
			String streamId = getOutputStreamId();
			logger.debug("{} provides {} tuples to stream {}",
					new Object[] {this, results.length, streamId});

			for (int i = results.length; --i >= 0; ) {
				Values entries = getMapping(results[i]);

				if (failSignature == null && ackSignature == null) {
					logger.trace("Tuple emit");
					collector.emit(streamId, entries);
					continue;
				}

				logger.trace("Transactional tuple emit");
				TransactionMessageId messageId = new TransactionMessageId();
				if (failSignature != null)
					messageId.setFailParams(mapOutputFields(results[i], failSignature.getArguments()));
				if (ackSignature != null)
					messageId.setAckParams(mapOutputFields(results[i], ackSignature.getArguments()));

				collector.emit(streamId, entries, messageId);
			}
		} catch (InvocationTargetException e) {
			Throwable cause = e.getCause();
			Class<? extends Throwable> causeType = cause.getClass();
			for (Map.Entry<Class<? extends Exception>,Long> option : delayExceptions.entrySet()) {
				if (option.getKey().isAssignableFrom(causeType)) {
					Long delay = option.getValue();
					logger.info("{} triggers a {}ms delay", causeType.getSimpleName(), delay);
					Utils.sleep(delay);
					return;
				}
			}

			collector.reportError(cause);
		} catch (IllegalAccessException e) {
			throw new SecurityException(e);
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
		if (! (o instanceof TransactionMessageId)) {
			logger.warn("Ack with unknown message ID: {}", o);
			return;
		}
		TransactionMessageId messageId = (TransactionMessageId) o;
		Object[] values = messageId.getAckParams();
		logger.trace("Ack with: {}", values);
		try {
			invoke(ackMethod, values);
		} catch (Exception e) {
			logger.error("Ack notification abort", e);
		}
	}

	@Override
	public void fail(Object o) {
		if (! (o instanceof TransactionMessageId)) {
			logger.warn("Fail with unknown message ID: {}", o);
			return;
		}
		TransactionMessageId messageId = (TransactionMessageId) o;
		Object[] values = messageId.getFailParams();
		logger.trace("Fail with: {}", values);
		try {
			invoke(failMethod, values);
		} catch (Exception e) {
			logger.error("Fail notification abort", e);
		}
	}

	/**
	 * Sets the method for transaction acknowledgement.
	 */
	public void setAckSignature(String value) {
		ackSignature = FunctionSignature.valueOf(value);
	}

	/**
	 * Sets the method for transaction failures.
	 */
	public void setFailSignature(String value) {
		failSignature = FunctionSignature.valueOf(value);
	}

	/**
	 * Sets the delays per exception.
	 * @see #putDelayException(Class, long)
	 */
	public void setDelayExceptions(Map<Class<? extends Exception>,Long> value) {
		delayExceptions.clear();
		for (Map.Entry<Class<? extends Exception>,Long> entry : value.entrySet())
			putDelayException(entry.getKey(), entry.getValue());
	}

	/**
	 * Registers a delay for an exception.
	 * When the invocation on the bean fails with a matching exception then
	 * {@link #nextTuple()} gets extended with {@link Utils#sleep(long)}.
	 * @param type the criteria.
	 * @param delay the number of milliseconds.
	 */
	public void putDelayException(Class<? extends Exception> type, long delay) {
		delayExceptions.put(type, delay);
	}

	@Override
	public String toString() {
		StringBuilder buffer = new StringBuilder("[spout '");
		buffer.append(getId()).append("']");
		return buffer.toString();
	}

}
