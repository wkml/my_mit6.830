package simpledb.transaction;

public class Lock {
    // 加锁的事务的ID
    private TransactionId tid;
    // 0 共享锁 1 独占锁
    // 0 读锁 1 写锁
    private int lockType;

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
