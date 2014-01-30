package eu.icolumbo.breeze;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;

import static java.lang.String.format;


/**
 * Spring for Storm spouts.
 * @author Jethro Bakker
 * @author Pascal S. de Kloe
 */
public class SpringSpout extends SpringComponent implements ConfiguredSpout {

	private static final Logger logger = LoggerFactory.getLogger(SpringSpout.class);

	private SpoutOutputCollector collector;

	private FunctionSignature ackSignature, failSignature;
	private transient Method ackMethod, failMethod;

	public SpringSpout(Class<?> beanType, String invocation, String... outputFields) {
		super(beanType, invocation, outputFields);
	}

	@Override
	public void open(Map stormConf, TopologyContext topologyContext, SpoutOutputCollector outputCollector) {
		collector = outputCollector;
		this.init(stormConf, topologyContext);
	}

	@Override
	public void nextTuple() {
		try {
			Object[] results = invoke(method);
			String streamId = getOutputStreamId();
			logger.debug("{} provides {} tuples to stream {}",
					new Object[] {this, results.length, streamId});

			for (int i = results.length; --i >= 0; ) {
				Values entries = getMapping(results[i], getOutputFields());

				TransactionMessageId messageId = new TransactionMessageId();
				if (failSignature != null)
					messageId.setFail(getTransactionMapping(results[i], failSignature.getArguments()));
				if (ackSignature != null) {
					messageId.setAck(getTransactionMapping(results[i], ackSignature.getArguments()));
				}

				collector.emit(streamId, entries, messageId);
			}
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
	public void ack(Object obj) {
		if (ackMethod != null) {
			Values values = ((TransactionMessageId) obj).getAck();
			invokeTransactionMethod(values, ackMethod);
		}
	}

	@Override
	public void fail(Object obj) {
		if (failMethod != null) {
			Values values = ((TransactionMessageId) obj).getFail();
			invokeTransactionMethod(values, failMethod);
		}
	}

	private void invokeTransactionMethod(Values values, Method method) {
		try {
			invoke(method, values.toArray());
		} catch (InvocationTargetException e) {
			logger.warn("Exception while invoking method", e);
		}
	}

	@Override
	protected void init(Map stormConf, TopologyContext topologyContext) {
		super.init(stormConf, topologyContext);

		if (ackSignature != null) {
			ackMethod = initTransactionMethod(ackSignature);
		}

		if (failSignature != null) {
			failMethod = initTransactionMethod(failSignature);
		}
	}

	private Method initTransactionMethod(FunctionSignature signature) {
		Method method;
		try {
			method = findMethod(beanType, signature.getFunction(), signature.getArguments().length);
			logger.info(format("%s uses %s for transaction", this, method.toGenericString()));
		} catch (ReflectiveOperationException e) {
			throw new IllegalStateException("Can't use configured bean method", e);
		}
		method.setAccessible(true);
		return method;
	}

	protected Values getTransactionMapping(Object returnEntry, String[] fields) throws InvocationTargetException {
		try {
			return new Values(mapOutputFields(returnEntry, fields));
		} catch (IllegalAccessException e) {
			throw new SecurityException(e);
		}
	}

	public void setAckSignature(String ack) {
		this.ackSignature = FunctionSignature.valueOf(ack);
	}

	public void setFailSignature(String fail) {
		this.failSignature = FunctionSignature.valueOf(fail);
	}

	public Method getAckMethod() {
		return ackMethod;
	}

	public Method getFailMethod() {
		return failMethod;
	}
}
