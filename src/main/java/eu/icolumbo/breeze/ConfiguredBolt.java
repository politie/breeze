package eu.icolumbo.breeze;

import org.apache.storm.topology.IRichBolt;


/**
 * @author Pascal S. de Kloe
 */
public interface ConfiguredBolt extends ConfiguredComponent, IRichBolt {

	/**
	 * Gets the field names.
	 */
	String[] getInputFields();

	/**
	 * Gets the field names which should be copied from the input tuple in addition to
	 * the {@link #getOutputFields() output fields}.
	 */
	String[] getPassThroughFields();

	/**
	 * Sets the field names which should be copied from the input tuple in addition to
	 * the {@link #getOutputFields() output fields}.
	 */
	void setPassThroughFields(String... value);

}
