package eu.icolumbo.breeze;


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

	public Data nullObject() {
		return null;
	}

	public String getGreeting() {
		return greeting;
	}

	public void setGreeting(String value) {
		greeting = value;
	}

}
