package simpledb.transaction;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

/**
 * TransactionId is a class that contains the identifier of a transaction.
 * 包含一个事务标识符的类
 */
public class TransactionId implements Serializable {

    private static final long serialVersionUID = 1L;

    // 初始ID为0，原子long保证了并发的绝对稳定自增
    static final AtomicLong counter = new AtomicLong(0);
    // 每一个事物的唯一ID标识符
    final long myId;

    public TransactionId() {
        myId = counter.getAndIncrement();
    }

    public long getId() {
        return myId;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        TransactionId other = (TransactionId) obj;
        return myId == other.myId;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (myId ^ (myId >>> 32));
        return result;
    }
}
