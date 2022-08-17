package simpledb.storage;

/**
 * Unique identifier for HeapPage objects.
 * 维护每一个页的唯一ID，包含tableId和page的数量
 * 一个heap page 存储的是一个表的部分信息
 */
public class HeapPageId implements PageId {

    /**
     * 一个table的Id，其实也是一个文件的Id，因为一个table对应一个文件
     */
    private final int tableId;

    /**
     * 页的编号， 0 表示是文件中的第0页，1 表示是文件中的第2页，以此类推
     */
    private final int pageNum;

    /**
     * Constructor. Create a page id structure for a specific page of a
     * specific table.
     *
     * @param tableId 表号（其实也是文件号，二者相同）
     * @param pgNo    页号，（注意不是页的数量）
     */
    public HeapPageId(int tableId, int pgNo) {
        // some code goes here
        this.tableId = tableId;
        this.pageNum = pgNo;
    }

    /**
     * @return the table associated with this PageId
     */
    public int getTableId() {
        // some code goes here
        return this.tableId;
    }

    /**
     * @return the page number in the table getTableId() associated with
     * this PageId
     */
    public int getPageNumber() {
        return this.pageNum;
    }

    /**
     * @return a hash code for this page, represented by the concatenation of
     * the table number and the page number (needed if a PageId is used as a
     * key in a hash table in the BufferPool, for example.)
     * @see BufferPool
     */

    @Override
    public int hashCode() {
        return 31 * tableId + pageNum;
    }

    /**
     * Compares one PageId to another.
     *
     * @param o The object to compare against (must be a PageId)
     * @return true if the objects are equal (e.g., page numbers and table
     * ids are the same)
     */
    public boolean equals(Object o) {
        // some code goes here
        if (o == this) {
            return true;
        }
        if (o instanceof PageId) {
            PageId another = (PageId) o;
            return this.pageNum == another.getPageNumber() && this.tableId == another.getTableId();
        } else
            return false;
    }

    /**
     * Return a representation of this object as an array of
     * integers, for writing to disk.  Size of returned array must contain
     * number of integers that corresponds to number of args to one of the
     * constructors.
     */
    public int[] serialize() {
        int data[] = new int[2];

        data[0] = getTableId();
        data[1] = getPageNumber();

        return data;
    }
}
