package eu.icolumbo.breeze;

import backtype.storm.topology.IComponent;


/**
 * @author Pascal S. de Kloe
 */
public interface ConfiguredComponent extends IComponent {

	/**
	 * Gets the Storm & Spring identifier.
	 */
	String getId();

	/**
	 * Gets the Storm identifier.
	 */
	String getOutputStreamId();

	/**
	 * Gets the field names.
	 */
	String[] getOutputFields();

	/**
	 * Gets the Strom parallelism hint.
	 */
	Number getParallelism();

	/**
	 * Gets an informal description for messaging purposes.
	 */
	@Override
	String toString();

}
