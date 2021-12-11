package simpledb.transaction;

public class Lock {
    private TransactionId tid;
    private int           lockType;

    public Lock(final TransactionId tid, final int lockType) {
        this.tid = tid;
        this.lockType = lockType;
    }

    public int getLockType() {
        return lockType;
    }

    public void setLockType(final int lockType) {
        this.lockType = lockType;
    }

    public TransactionId getTid() {
        return tid;
    }
}
