package eu.icolumbo.breeze;

import java.io.Serializable;


/**
 * Transaction context container.
 * @author Jethro Bakker
 * @author Pascal S. de Kloe
 */
public class TransactionMessageId implements Serializable {

	private static final long serialVersionUID = 2L;

	private Object[] ackParams, failParams;


	/**
	 * Gets the parameter values for the
	 * {@link SpringSpout#setAckSignature(String) transaction ack call}.
	 */
	public Object[] getAckParams() {
		return ackParams;
	}

	/**
	 * Sets the parameter values for the
	 * {@link SpringSpout#setAckSignature(String) transaction ack call}.
	 */
	public void setAckParams(Object... values) {
		ackParams = values;
	}

	/**
	 * Gets the parameter values for the
	 * {@link SpringSpout#setFailSignature(String) transaction fail call}.
	 */
	public Object[] getFailParams() {
		return failParams;
	}

	/**
	 * Sets the parameter values for the
	 * {@link SpringSpout#setFailSignature(String) transaction fail call}.
	 */
	public void setFailParams(Object... values) {
		failParams = values;
	}

}
