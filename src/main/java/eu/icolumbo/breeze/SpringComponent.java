package eu.icolumbo.breeze;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IComponent;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.StringTokenizer;

import static java.lang.String.format;


/**
 * Spring for Storm components.
 * @author Pascal S. de Kloe
 * @author Jethro Bakker
 */
public abstract class SpringComponent implements IComponent, ApplicationContextAware {

	private static final Logger logger = LoggerFactory.getLogger(SpringSpout.class);
	private static final long serialVersionUID = 1;
	private static final Values[] EMPTY_ARRAY = new Values[0];

	private final Class<?> beanType;
	private final String methodName;
	protected final String[] inputFields, outputFields;

	private transient String id;
	private transient ApplicationContext spring;
	transient Method method;

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

		int paramStart = invocation.indexOf("(");
		int paramEnd = invocation.indexOf(")");
		if (paramStart < 0 || paramEnd != invocation.length() - 1)
			throw new IllegalArgumentException("Malformed method signature: " + invocation);
		this.methodName = invocation.substring(0, paramStart);
		String arguments = invocation.substring(paramStart + 1, paramEnd);

		StringTokenizer tokenizer = new StringTokenizer(arguments, ", ");
		this.inputFields = new String[tokenizer.countTokens()];
		for (int i = 0; tokenizer.hasMoreTokens();
			 this.inputFields[i++] = tokenizer.nextToken());
	}

	/**
	 * Instantiates the non-serializable state.
	 */
	protected void init(Map stormConf, TopologyContext topologyContext) {
		id = topologyContext.getThisComponentId();
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
		declarer.declare(new Fields(outputFields));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	@Override
	public void setApplicationContext(ApplicationContext value) {
		spring = value;
	}

	/**
	 * Gets an informal description for messaging purposes.
	 */
	@Override
	public String toString() {
		StringBuilder description = new StringBuilder();
		description.append(getClass().getSimpleName());
		description.append(" '").append(id).append('\'');
		return description.toString();
	}

	/**
	 * Gets the bean invocation mapping.
	 */
	protected Values[] invoke(Object... arguments)
	throws InvocationTargetException {
		Object[] returnEntries;
		try {
			Object bean = spring.getBean(beanType);
			Object returnValue = method.invoke(bean, arguments);

			if (outputFields.length == 0) return EMPTY_ARRAY;

			if (scatterOutput)
				returnEntries = scatter(returnValue);
			else
				returnEntries = new Object[] {returnValue};
		} catch (IllegalAccessException e) {
			throw new SecurityException(e);
		}

		int i = returnEntries.length;
		Values[] mapping = new Values[i];
		while (--i >= 0) {
			Object entry = returnEntries[i];
			if (outputFields.length == 1) {
				mapping[i] = new Values(entry);
			} else {
				int j = outputFields.length;
				Object[] output = new Object[j];

				if (entry instanceof Map) {
					Map<?,?> map = (Map<?,?>) entry;
					while (--j >= 0)
						output[j] = map.get(outputFields[j]);
				}

				mapping[i] = new Values(output);
			}
		}
		return mapping;
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

	/**
	 * Gets the Storm & Spring identifier.
	 */
	public String getId() {
		return id;
	}

	/**
	 * Sets the Storm & Spring identifier.
	 */
	public void setId(String value) {
		id = value;
	}

	/**
	 * Gets the field names.
	 */
	public String[] getInputFields() {
		return inputFields;
	}

	/**
	 * Gets the field names.
	 */
	public String[] getOutputFields() {
		return outputFields;
	}

	/**
	 * Sets whether items in collection and array returns
	 * should be emitted as individual output tuples.
	 */
	public void setScatterOutput(boolean value) {
		scatterOutput = value;
	}

}
