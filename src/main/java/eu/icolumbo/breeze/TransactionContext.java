package eu.icolumbo.breeze;

public class TransactionContext {

    private Object[] failParams;
    private Object[] ackParams;

    public Object[] getFailParams() {
        return failParams;
    }

    public void setFailParams(Object[] failParams) {
        this.failParams = failParams;
    }

    public Object[] getAckParams() {
        return ackParams;
    }

    public void setAckParams(Object[] ackParams) {
        this.ackParams = ackParams;
    }
}