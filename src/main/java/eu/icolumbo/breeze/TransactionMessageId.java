package eu.icolumbo.breeze;

import backtype.storm.tuple.Values;

import java.io.Serializable;

/**
 * Transaction message object.
 * @author Jethro Bakker
 */
public class TransactionMessageId implements Serializable {

	private static final Long serialVersionUID = 1L;

	private Values ack;

	private Values fail;

	public void setAck(Values ack) {
		this.ack = ack;
	}

	public void setFail(Values fail) {
		this.fail = fail;
	}

	public Values getAck() {
		return ack;
	}

	public Values getFail() {
		return fail;
	}
}
