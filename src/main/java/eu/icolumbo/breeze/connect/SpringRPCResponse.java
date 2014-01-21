package eu.icolumbo.breeze.connect;

import eu.icolumbo.breeze.ConfiguredBolt;
import eu.icolumbo.breeze.FunctionSignature;

import backtype.storm.drpc.ReturnResults;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static backtype.storm.utils.Utils.DEFAULT_STREAM_ID;
import static java.lang.String.format;


/**
 * @author Pascal S. de Kloe
 */
public class SpringRPCResponse extends ReturnResults implements ConfiguredBolt {

	private static final String[] outputFields = {};

	private final String ID;
	private final String[] inputFields;
	private final String DESCRIPTION;

	private String[] passThroughFields = {};
	private boolean doAnchor = true;
	private Number parallelism;

	private OutputCollector collector;


	public SpringRPCResponse(String signature, String... returnFields) {
		this(FunctionSignature.valueOf(signature), returnFields);
	}

	private SpringRPCResponse(FunctionSignature signature, String... returnFields) {
		ID = signature.getFunction() + "-rpc-rsp";
		DESCRIPTION = format("%s '%s'", getClass().getSimpleName(), signature.getFunction());

		if (returnFields.length != 1) {
			String msg = "Breeze RPC requires 1 output field for now";
			throw new UnsupportedOperationException(msg);
		}

		String returnField = returnFields[0];
		String requestContextField = SpringRPCRequest.getContextField(signature);
		this.inputFields = new String[] {returnField, requestContextField};
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		Fields fields = new Fields(getPassThroughFields());
		declarer.declareStream(getOutputStreamId(), fields);
	}

	@Override
	public void execute(Tuple input) {
		List<Object> values = new ArrayList<>();
		for (String field : passThroughFields)
			values.add(input.getValueByField(field));

		if (doAnchor)
			collector.emit(getOutputStreamId(), input, values);
		else
			collector.emit(getOutputStreamId(), values);

		super.execute(input);
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		super.prepare(stormConf, context, collector);
	}

	@Override
	public String toString() {
		return DESCRIPTION;
	}

	@Override
	public String getId() {
		return ID;
	}

	@Override
	public String getOutputStreamId() {
		return DEFAULT_STREAM_ID;
	}

	@Override
	public String[] getInputFields() {
		return inputFields;
	}

	@Override
	public String[] getOutputFields() {
		return outputFields;
	}

	/**
	 * Sets whether the tuple should be replayed in case of an error.
	 * @see <a href="https://github.com/nathanmarz/storm/wiki/Guaranteeing-message-processing">Storm Wiki</a>
	 */
	public void setDoAnchor(boolean value) {
		doAnchor = value;
	}

	@Override
	public String[] getPassThroughFields() {
		return passThroughFields;
	}

	@Override
	public void setPassThroughFields(String... value) {
		passThroughFields = value;
	}

	@Override
	public Number getParallelism() {
		return parallelism;
	}

	public void setParallelism(Number value) {
		parallelism = value;
	}

}
