package eu.icolumbo.breeze;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
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

import static org.apache.storm.utils.Utils.DEFAULT_STREAM_ID;
import static org.springframework.expression.spel.SpelMessage.PROPERTY_OR_FIELD_NOT_READABLE;


/**
 * Spring for Storm components.
 * @author Pascal S. de Kloe
 * @author Jethro Bakker
 */
public abstract class SpringComponent implements ConfiguredComponent, ApplicationContextAware {

	private static final Logger logger = LoggerFactory.getLogger(SpringComponent.class);
	private static final SpelExpressionParser expressionParser = new SpelExpressionParser();
	private static final long serialVersionUID = 3;
	static final Values[] EMPTY_ARRAY = {};

	protected final Class<?> beanType;

	private final FunctionSignature inputSignature;
	private final String[] outputFields;
	private final Map<String,String> outputBindingDefinitions = new HashMap<>();
	private final Map<String,Expression> outputBinding = new HashMap<>();

	private String outputStreamId;
	private boolean scatterOutput;
	private Number parallelism;

	private transient String id;
	private transient ApplicationContext spring;
	private transient Method method;


	/**
	 * Convenience constructor.
	 * @param beanType the identification.
	 * @param invocation the method signature including input field names.
	 * @param outputFields the names.
	 */
	public SpringComponent(Class<?> beanType, String invocation, String... outputFields) {
		this.beanType = beanType;
		this.inputSignature = FunctionSignature.valueOf(invocation);
		this.outputFields = outputFields;
	}

	/**
	 * Instantiates the non-serializable state.
	 */
	protected void init(Map stormConf, TopologyContext topologyContext) {
		setId(topologyContext.getThisComponentId());

		try {
			method = inputSignature.findMethod(beanType);
			logger.info("{} uses {}", this, method.toGenericString());
		} catch (ReflectiveOperationException e) {
			throw new IllegalStateException("Unusable input signature", e);
		}

		if (spring == null)
			spring = SingletonApplicationContext.get(stormConf, topologyContext);

		spring.getBean(beanType);
		logger.debug("Bean lookup successful");
	}

	/**
	 * Registers the {@link #getOutputFields() output field names}.
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		String streamId = getOutputStreamId();
		Fields names = new Fields(outputFields);
		logger.info("{} declares {} for stream '{}'",
				new Object[] {this, names, streamId});
		declarer.declareStream(streamId, names);
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	/**
	 * Gets the bean invocation return entries.
	 */
	protected Object[] invoke(Object[] arguments)
	throws InvocationTargetException, IllegalAccessException {
		Object returnValue = invoke(method, arguments);

		if (! scatterOutput) {
			logger.trace("Using return as is");
			return new Object[] {returnValue};
		}

		if (returnValue instanceof Object[]) {
			logger.trace("Scatter array return");
			return (Object[]) returnValue;
		}

		if (returnValue instanceof Collection) {
			logger.trace("Scatter collection return");
			return ((Collection) returnValue).toArray();
		}

		logger.debug("Scatter singleton return");
		return returnValue == null ? EMPTY_ARRAY : new Object[] {returnValue};
	}

	/**
	 * Gets the bean invocation return value.
	 */
	protected Object invoke(Method method, Object[] arguments)
	throws InvocationTargetException, IllegalAccessException {
		logger.trace("Lookup for call {}", method);
		Object bean = spring.getBean(beanType);

		try {
			return method.invoke(bean, arguments);
		} catch (IllegalArgumentException e) {
			StringBuilder msg = new StringBuilder(method.toGenericString());
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
	}

	protected Values getMapping(Object returnEntry) {
		return new Values(mapOutputFields(returnEntry, outputFields));
	}

	protected Object[] mapOutputFields(Object returnEntry, String[] fields) {
		StandardEvaluationContext context = new StandardEvaluationContext(returnEntry);

		int i = fields.length;
		Object[] output = new Object[i];
		while (--i >= 0) {
			try {
				Expression spel = getOutputBinding(fields[i]);
				output[i] = spel.getValue(context);
			} catch (SpelEvaluationException e) {
				if (e.getMessageCode() != PROPERTY_OR_FIELD_NOT_READABLE)
					throw e;
				logger.info(e.getMessage());
			}
		}

		return output;
	}

	@Override
	public String getOutputStreamId() {
		String value = outputStreamId;
		if (value == null) {
			value = DEFAULT_STREAM_ID;
			setOutputStreamId(value);
		}
		return value;
	}

	/**
	 * Sets the Storm identifier.
	 */
	public void setOutputStreamId(String value) {
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
	 * Sets expressions per field.
	 * @see #putOutputBinding(String, String)
	 */
	public void setOutputBinding(Map<String,String> value) {
		outputBinding.clear();
		outputBindingDefinitions.clear();
		for (Map.Entry<String,String> entry : value.entrySet())
			putOutputBinding(entry.getKey(), entry.getValue());
	}

	/**
	 * Registers an expression for a field.
	 * @param field the name.
	 * @param expression the SpEL definition.
	 */
	public void putOutputBinding(String field, String expression) {
		outputBindingDefinitions.put(field, expression);
	}

	private Expression getOutputBinding(String field) {
		Expression binding = outputBinding.get(field);
		if (binding == null) {
			String definition = outputBindingDefinitions.get(field);
			if (definition == null) {
				if (outputFields.length == 1 && outputFields[0].equals(field))
					definition = "#root";
				else
					definition = "#root?." + field;
			}
			logger.debug("Field {} bound as #{{}}", field, definition);
			binding = expressionParser.parseExpression(definition);
			outputBinding.put(field, binding);
		}
		return binding;
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
		return id;
	}

	@Override
	public void setId(String value) {
		id = value;
	}

	@Override
	public void setApplicationContext(ApplicationContext value) {
		spring = value;
	}

}
