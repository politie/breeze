package eu.icolumbo.breeze;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;

import static java.lang.String.format;
import static backtype.storm.utils.Utils.DEFAULT_STREAM_ID;
import static org.springframework.beans.BeanUtils.getPropertyDescriptor;


/**
 * Spring for Storm components.
 * @author Pascal S. de Kloe
 * @author Jethro Bakker
 */
public abstract class SpringComponent implements ConfiguredComponent, ApplicationContextAware {

	private static final Logger logger = LoggerFactory.getLogger(SpringSpout.class);
	private static final long serialVersionUID = 2;
	private static final Values[] EMPTY_ARRAY = {};

	private final Class<?> beanType;
	private final String methodName;
	private final String[] inputFields, outputFields;

	private transient String id;
	private String outputStreamId;
	private Number parallelism;

	private transient ApplicationContext spring;
	private transient Method method;

	private boolean scatterOutput;


	/**
	 * Convenience constructor.
	 * @param beanType the identification.
	 * @param invocation the method signature including input field names.
	 * @param outputFields the names.
	 */
	public SpringComponent(Class<?> beanType, String invocation, String... outputFields) {
		this.beanType = beanType;
		this.outputFields = outputFields;

		FunctionSignature signature = FunctionSignature.valueOf(invocation);
		this.methodName = signature.getFunction();
		this.inputFields = signature.getArguments();
	}

	/**
	 * Instantiates the non-serializable state.
	 */
	protected void init(Map stormConf, TopologyContext topologyContext) {
		setId(topologyContext.getThisComponentId());
		logger.debug("Prepare " + this);

		try {
			method = findMethod(beanType, methodName, inputFields.length);
			logger.info(format("%s uses %s", this, method.toGenericString()));
		} catch (ReflectiveOperationException e) {
			throw new IllegalStateException("Can't use configured bean method", e);
		}

		if (spring == null)
			spring = SingletonApplicationContext.get(stormConf, topologyContext);

		// Fail-fast
		method.setAccessible(true);
		spring.getBean(beanType);
	}

	/**
	 * Registers the {@link #getOutputFields() output field names}.
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(getOutputStreamId(), new Fields(outputFields));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	@Override
	public void setApplicationContext(ApplicationContext value) {
		spring = value;
	}

	@Override
	public String toString() {
		StringBuilder description = new StringBuilder();
		description.append(getClass().getSimpleName());
		description.append(" '").append(getId()).append('\'');
		return description.toString();
	}

	/**
	 * Gets the bean invocation mapping.
	 */
	protected Values[] invoke(Object... arguments)
	throws InvocationTargetException {
		try {
			Object bean = spring.getBean(beanType);

			Object[] returnEntries;
			try {
				logger.trace("Invoking {} on {}", method, bean);
				Object returnValue = method.invoke(bean, arguments);

				if (outputFields.length == 0) return EMPTY_ARRAY;

				if (scatterOutput) {
					returnEntries = scatter(returnValue);
					logger.trace("Scattered {} into {} parts", returnValue, returnEntries.length);
				} else {
					returnEntries = new Object[] {returnValue};
					logger.trace("Using return {}", returnValue);
				}
			} catch (IllegalArgumentException e) {
				StringBuilder msg = new StringBuilder(toString());
				msg.append(" invoked with incompatible arguments:");
				for (Object a : arguments) {
					msg.append(' ');
					if (a == null)
						msg.append("null");
					else
						msg.append(a.getClass().getName());
				}
				logger.error(msg.toString());
				throw e;
			}

			int i = returnEntries.length;
			Values[] mapping = new Values[i];
			while (--i >= 0) {
				Object entry = returnEntries[i];
				if (outputFields.length == 1)
					mapping[i] = new Values(entry);
				else
					mapping[i] = new Values(mapMultipleOutputFields(entry));
			}
			return mapping;
		} catch (IllegalAccessException e) {
			throw new SecurityException(e);
		}
	}

	private Object[] mapMultipleOutputFields(Object result) throws IllegalAccessException, InvocationTargetException {
		int i = outputFields.length;
		Object[] output = new Object[i];

		if (result instanceof Map) {
			Map<?,?> map = (Map<?,?>) result;
			while (--i >= 0)
				output[i] = map.get(outputFields[i]);
		} else if (result != null) {
			while (--i >= 0) {
				String name = outputFields[i];
				PropertyDescriptor descriptor = getPropertyDescriptor(result.getClass(), name);
				if (descriptor == null) {
					logger.warn("Missing property '{}' on {} for {}",
							new Object[] {name, result.getClass(), this});
					continue;
				}
				Method method = descriptor.getReadMethod();
				if (method == null) {
					logger.warn("Missing property '{}' getter on {} for {}",
							new Object[] {name, result.getClass(), this});
					continue;
				}
				output[i] = method.invoke(result);
			}
		}

		return output;
	}

	private static Object[] scatter(Object o) {
		if (o == null) return EMPTY_ARRAY;
		if (o instanceof Object[])
			return (Object[]) o;
		if (o instanceof Collection)
			return ((Collection) o).toArray();
		return new Object[] {o};
	}

	static Method findMethod(Class<?> type, String name, int paramCount)
	throws ReflectiveOperationException {
		Method match = null;
		for (Method option : type.getDeclaredMethods()) {
			if (! name.equals(option.getName())) continue;
			Class<?>[] parameters = option.getParameterTypes();
			if (parameters.length != paramCount) continue;
			if (match != null) {
				if (refines(option, match)) continue;
				if (! refines(match, option))
					throw ambiguity(match, option);
			}
			match = option;
		}

		Class<?>[] parents = { type.getSuperclass() };
		if (type.isInterface())
			parents = type.getInterfaces();
		for (Class<?> parent : parents) {
			if (parent == null) continue;
			try {
				Method superMatch = findMethod(parent, name, paramCount);
				if (match == null) {
					match = superMatch;
					continue;
				}
				if (! refines(superMatch, match))
					throw ambiguity(match, superMatch);
			} catch (NoSuchMethodException ignored) {
			}
		}

		if (match != null) return match;
		String msg = format("No method %s#%s with %d parameters",
				type, name, paramCount);
		throw new NoSuchMethodException(msg);
	}

	private static boolean refines(Method a, Method b) {
		Class<?>[] aParams = a.getParameterTypes();
		Class<?>[] bParams = b.getParameterTypes();
		int i = aParams.length;
		while (--i >= 0)
			if (! bParams[i].isAssignableFrom(aParams[i]))
				return false;
		return true;
	}

	private static ReflectiveOperationException ambiguity(Method a, Method b)
	throws ReflectiveOperationException {
		String[] readable = { a.toGenericString(), b.toGenericString() };
		Arrays.sort(readable);
		String msg = format("Ambiguity between %s and %s",
				readable[0], readable[1]);
		return new ReflectiveOperationException(msg);
	}

	@Override
	public String getId() {
		if (id == null) {
			setId(UUID.randomUUID().toString());
			logger.warn("Generated ID for {}: {}", this, beanType);
		}
		return id;
	}

	/**
	 * Sets the Storm & Spring identifier.
	 */
	public void setId(String value) {
		id = value;
	}

	@Override
	public String getOutputStreamId() {
		if (outputStreamId == null)
			setOutputStreamId(DEFAULT_STREAM_ID);
		return outputStreamId;
	}

	/**
	 * Sets the Storm identifier.
	 */
	public void setOutputStreamId(String value) {
		logger.debug("{} output stream set to '{}'", this, value);
		outputStreamId = value;
	}

	@Override
	public Number getParallelism() {
		return parallelism;
	}

	/**
	 * Sets the Storm parallelism hint.
	 */
	public void setParallelism(Number value) {
		parallelism = value;
	}

	/**
	 * Gets the field names.
	 */
	public String[] getInputFields() {
		return inputFields;
	}

	@Override
	public String[] getOutputFields() {
		return outputFields;
	}

	/**
	 * Gets whether items in collection and array returns
	 * should be emitted as individual output tuples.
	 */
	public boolean getScatterOutput() {
		return scatterOutput;
	}

	/**
	 * Sets whether items in collection and array returns
	 * should be emitted as individual output tuples.
	 */
	public void setScatterOutput(boolean value) {
		scatterOutput = value;
	}

}
