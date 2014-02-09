package eu.icolumbo.breeze;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.StringTokenizer;

import static java.lang.String.format;


/**
 * Method binding for beans.
 * @author Pascal S. de Kloe
 * @author Jethro Bakker
 */
public class FunctionSignature implements Serializable {

	private static final long serialVersionUID = 2L;

	private final String FUNCTION;
	private final String[] arguments;


	public FunctionSignature(String function, String... arguments) {
		this.FUNCTION = function;
		this.arguments = arguments;
	}

	/**
	 * Parses a signature.
	 */
	public static FunctionSignature valueOf(String serial) {
		int paramStart = serial.indexOf("(");
		int paramEnd = serial.indexOf(")");
		if (paramStart < 0 || paramEnd != serial.length() - 1)
			throw new IllegalArgumentException("Malformed method signature: " + serial);

		String function = serial.substring(0, paramStart).trim();

		String arguments = serial.substring(paramStart + 1, paramEnd);
		StringTokenizer tokenizer = new StringTokenizer(arguments, ", ");
		String[] names = new String[tokenizer.countTokens()];
		for (int i = 0; i < names.length; ++i)
			 names[i] = tokenizer.nextToken();

		return new FunctionSignature(function, names);
	}

	/**
	 * Gets the name;
	 */
	public String getFunction() {
		return FUNCTION;
	}

	/**
	 * Gets the names.
	 */
	public String[] getArguments() {
		return arguments;
	}

	/**
	 * Gets a method match.
	 */
	public Method findMethod(Class<?> type)	throws ReflectiveOperationException {
		Method match = null;
		for (Method option : type.getDeclaredMethods()) {
			if (! getFunction().equals(option.getName())) continue;
			Class<?>[] parameters = option.getParameterTypes();
			if (parameters.length != getArguments().length) continue;
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
				Method superMatch = findMethod(parent);
				if (match == null) {
					match = superMatch;
					continue;
				}
				if (! refines(superMatch, match))
					throw ambiguity(match, superMatch);
			} catch (NoSuchMethodException ignored) {
			}
		}

		if (match == null) {
			String fmt = "No method %s#%s with %d parameters";
			String msg = format(fmt, type, getFunction(), getArguments().length);
			throw new NoSuchMethodException(msg);
		}

		match.setAccessible(true);
		return match;
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

}
