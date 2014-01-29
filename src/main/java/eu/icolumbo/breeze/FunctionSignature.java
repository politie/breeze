package eu.icolumbo.breeze;

import java.io.Serializable;
import java.util.StringTokenizer;


/**
 * @author Pascal S. de Kloe
 * @author Jethro Bakker
 */
public class FunctionSignature implements Serializable {

	private String function;
	private String[] arguments;


	/**
	 * Parses the signature.
	 */
	public static FunctionSignature valueOf(String serial) {
		FunctionSignature result = new FunctionSignature();

		int paramStart = serial.indexOf("(");
		int paramEnd = serial.indexOf(")");
		if (paramStart < 0 || paramEnd != serial.length() - 1)
			throw new IllegalArgumentException("Malformed method signature: " + serial);

		result.function = serial.substring(0, paramStart).trim();

		String arguments = serial.substring(paramStart + 1, paramEnd);
		StringTokenizer tokenizer = new StringTokenizer(arguments, ", ");
		result.arguments = new String[tokenizer.countTokens()];
		for (int i = 0; tokenizer.hasMoreTokens();
			 result.arguments[i++] = tokenizer.nextToken());
		return result;
	}

	/**
	 * Gets the name;
	 */
	public String getFunction() {
		return function;
	}

	/**
	 * Gets the argument names.
	 */
	public String[] getArguments() {
		return arguments;
	}

}
