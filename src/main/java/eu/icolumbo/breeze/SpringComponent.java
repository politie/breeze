package eu.icolumbo.breeze;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.SpelEvaluationException;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static backtype.storm.utils.Utils.DEFAULT_STREAM_ID;
import static java.lang.String.format;


/**
 * Spring for Storm components.
 * @author Pascal S. de Kloe
 * @author Jethro Bakker
 */
public abstract class SpringComponent implements ConfiguredComponent, ApplicationContextAware {

	private static final Logger logger = LoggerFactory.getLogger(SpringSpout.class);
	private static final SpelExpressionParser expressionParser = new SpelExpressionParser();
	private static final long serialVersionUID = 3;
	static final Values[] EMPTY_ARRAY = {};

	protected final Class<?> beanType;

	private final FunctionSignature inputSignature;
	private final String[] outputFields;

	private String outputStreamId;
	private boolean scatterOutput;

	private Number parallelism;

	private transient String id;
	private transient ApplicationContext spring;
	private transient Method method;
	private transient final Map<String,Expression> outputBinding = new HashMap<>();


	/**
	 * Convenience constructor.
	 * @param beanType the identification.
	 * @param invocation the method signature including input field names.
	 * @param outputFields the names.
	 */
	public SpringComponent(Class<?> beanType, String invocation, String... outputFields) {
		this.beanType = beanType;
		this.outputFields = outputFields;
		this.inputSignature = FunctionSignature.valueOf(invocation);
	}

	/**
	 * Instantiates the non-serializable state.
	 */
	protected void init(Map stormConf, TopologyContext topologyContext) {
		setId(topologyContext.getThisComponentId());
		logger.debug("Prepare " + this);

		try {
			method = inputSignature.findMethod(beanType);
			logger.info(format("%s uses %s", this, method.toGenericString()));
		} catch (ReflectiveOperationException e) {
			throw new IllegalStateException("Unusable input signature", e);
		}

		if (spring == null)
			spring = SingletonApplicationContext.get(stormConf, topologyContext);

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
	public String toString() {
		StringBuilder description = new StringBuilder();
		description.append(getClass().getSimpleName());
		description.append(" '").append(getId()).append('\'');
		return description.toString();
	}

	/**
	 * Gets the bean invocation mapping.
	 */
	protected Object[] invoke(Object[] arguments)
	throws InvocationTargetException, IllegalAccessException {
		return invoke(method, arguments);
	}

	/**
	 * Gets the bean invocation mapping.
	 */
	protected Object[] invoke(Method method, Object[] arguments)
	throws InvocationTargetException, IllegalAccessException {
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

		return returnEntries;
	}

	protected Values getMapping(Object returnEntry, String[] fields) {
		return new Values(mapOutputFields(returnEntry, fields));
	}

	protected Object[] mapOutputFields(Object returnEntry, String[] fields) {
		StandardEvaluationContext context = new StandardEvaluationContext(returnEntry);

		int i = fields.length;
		Object[] output = new Object[i];
		while (--i >= 0) try {
			Expression spel = getOutputBinding(fields[i]);
			output[i] = spel.getValue(context);
		} catch (SpelEvaluationException e) {
			switch (e.getMessageCode()) {
				case PROPERTY_OR_FIELD_NOT_READABLE:
					logger.info(e.getMessage());
					break;
				default:
					throw e;
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

	/**
	 * Gets the field names.
	 */
	public String[] getInputFields() {
		return inputSignature.getArguments();
	}

	@Override
	public String[] getOutputFields() {
		return outputFields;
	}

	/**
	 * Registers an expression for a field.
	 * @param field the name.
	 * @param expression the SpEL definition.
	 */
	public void addOutputBinding(String field, String expression) {
		logger.debug("Field {} bound as #{{}}", field, expression);
		Expression spel = expressionParser.parseExpression(expression);
		outputBinding.put(field, spel);
	}

	private Expression getOutputBinding(String field) {
		Expression binding = outputBinding.get(field);
		if (binding == null) {
			addOutputBinding(field, getDefaultExpression(field));
			binding = outputBinding.get(field);
		}
		return binding;
	}

	private String getDefaultExpression(String field) {
		if (outputFields.length == 1 && outputFields[0].equals(field))
			return "#root";
		return "#root?." + field;
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
	public void setApplicationContext(ApplicationContext value) {
		spring = value;
	}

}
