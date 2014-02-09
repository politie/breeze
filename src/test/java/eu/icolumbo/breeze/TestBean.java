package eu.icolumbo.breeze;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;


/**
 * @author Pascal S. de Kloe
 */
public class TestBean {

	private String greeting;


	public static class Data {
		private int id;
		private String message;

		public int getId() {
			return id;
		}

		public void setId(int value) {
			id = value;
		}

		public String getMessage() {
			return message;
		}

		public void setMessage(String value) {
			message = value;
		}

		public void setSetterOnly(Object value) {
		}

		private Object getPrivateGetter() {
			return new Object();
		}
	}


	public void nop() {
	}

	public String ping() {
		return "ping";
	}

	public String echo(String x) {
		return x;
	}

	public String[] array(String a, String b) {
		return new String[] {a, b};
	}

	public List<Object> list(Object a, Object b) {
		return asList(a, b);
	}

	public Map<String,String> map(String a, String b) {
		Map<String,String> result = new HashMap<>();
		result.put("x", a);
		result.put("y", b);
		return result;
	}

	public Data nullObject() {
		return null;
	}

	public Data[] nullArray() {
		return null;
	}

	public Data greet(Number number) {
		Data data = new Data();
		data.setId(number.intValue());
		data.setMessage(getGreeting() + " " + number);
		return data;
	}

	public String getGreeting() {
		return greeting;
	}

	public void setGreeting(String value) {
		greeting = value;
	}

}
