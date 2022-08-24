package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Catalog;
import simpledb.transaction.TransactionId;
import simpledb.util.IteratorWrapper;

import java.util.*;
import java.io.*;

/**
 * Each instance of HeapPage stores data for one page of HeapFiles and
 * implements the Page interface that is used by BufferPool.
 * 一个heap page 存储的是一个表的部分信息
 *
 * @see HeapFile
 * @see BufferPool
 */
public class HeapPage implements Page {

    // 一个页的ID，包含这个页对应的表的ID，以及这个表的页总数
    final HeapPageId pid;
    // 这个页的表的结构
    final TupleDesc td;
    // 维护的是一个bitmap
    final byte[] header;
    // 这个页存放的所有行
    final Tuple[] tuples;
    // 槽的数量
    final int numSlots;

    // 刷脏用的
    byte[] oldData;
    private final Byte oldDataLock = (byte) 0;

    // 如果一个事务修改了这个页，记录这个事务的ID
    private TransactionId tid;

    /**
     * Create a HeapPage from a set of bytes of data read from disk.
     * The format of a HeapPage is a set of header bytes indicating
     * the slots of the page that are in use, some number of tuple slots.
     * Specifically, the number of tuples is equal to: <p>
     * floor((BufferPool.getPageSize()*8) / (tuple size * 8 + 1))
     * <p> where tuple size is the size of tuples in this
     * database table, which can be determined via {@link Catalog#getTupleDesc}.
     * The number of 8-bit header words is equal to:
     * <p>
     * ceiling(no. tuple slots / 8)
     * <p>
     *
     * @see Database#getCatalog
     * @see Catalog#getTupleDesc
     * @see BufferPool#getPageSize()
     */
    public HeapPage(HeapPageId id, byte[] data) throws IOException {
        this.pid = id;
        // 获取这个表的字段信息
        this.td = Database.getCatalog().getTupleDesc(id.getTableId());
        // 获取这个页行的数量
        this.numSlots = getNumTuples();
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

        // allocate and read the header slots of this page
        header = new byte[getHeaderSize()];
        for (int i = 0; i < header.length; i++)
            header[i] = dis.readByte();

        tuples = new Tuple[numSlots];
        try {
            // allocate and read the actual records of this page
            for (int i = 0; i < tuples.length; i++)
                tuples[i] = readNextTuple(dis, i);
        } catch (NoSuchElementException e) {
            e.printStackTrace();
        }
        dis.close();

        setBeforeImage();
    }

    /**
     * Retrieve the number of tuples on this page.
     *
     * @return the number of tuples on this page
     */
    private int getNumTuples() {
        // some code goes here
        // 返回这个页一共存得下几行
        // 4096 * 8 / td.getSize() * 8 + 1
        return (BufferPool.getPageSize() * 8) / (td.getSize() * 8 + 1);
    }

    /**
     * Computes the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     *
     * @return the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     */
    private int getHeaderSize() {
        // some code goes here
        return (int) Math.ceil(getNumTuples() / 8.0);

    }

    /**
     * Return a view of this page before it was modified
     * -- used by recovery
     */
    public HeapPage getBeforeImage() {
        try {
            byte[] oldDataRef = null;
            synchronized (oldDataLock) {
                oldDataRef = oldData;
            }
            return new HeapPage(pid, oldDataRef);
        } catch (IOException e) {
            e.printStackTrace();
            //should never happen -- we parsed it OK before!
            System.exit(1);
        }
        return null;
    }

    public void setBeforeImage() {
        synchronized (oldDataLock) {
            oldData = getPageData().clone();
        }
    }

    /**
     * @return the PageId associated with this page.
     */
    public HeapPageId getId() {
        // some code goes here
        return this.pid;
    }

    /**
     * Suck up tuples from the source file.
     */
    private Tuple readNextTuple(DataInputStream dis, int slotId) throws NoSuchElementException {
        // if associated bit is not set, read forward to the next tuple, and return null.
        if (!isSlotUsed(slotId)) {
            for (int i = 0; i < td.getSize(); i++) {
                try {
                    dis.readByte();
                } catch (IOException e) {
                    throw new NoSuchElementException("error reading empty tuple");
                }
            }
            return null;
        }

        // read fields in the tuple
        Tuple t = new Tuple(td);
        RecordId rid = new RecordId(pid, slotId);
        t.setRecordId(rid);
        try {
            for (int j = 0; j < td.numFields(); j++) {
                Field f = td.getFieldType(j).parse(dis);
                t.setField(j, f);
            }
        } catch (java.text.ParseException e) {
            e.printStackTrace();
            throw new NoSuchElementException("parsing error!");
        }

        return t;
    }

    /**
     * Generates a byte array representing the contents of this page.
     * Used to serialize this page to disk.
     * <p>
     * The invariant here is that it should be possible to pass the byte
     * array generated by getPageData to the HeapPage constructor and
     * have it produce an identical HeapPage object.
     *
     * @return A byte array correspond to the bytes of this page.
     * @see #HeapPage
     */
    public byte[] getPageData() {
        int len = BufferPool.getPageSize();
        ByteArrayOutputStream baos = new ByteArrayOutputStream(len);
        DataOutputStream dos = new DataOutputStream(baos);

        // create the header of the page
        for (int i = 0; i < header.length; i++) {
            try {
                dos.writeByte(header[i]);
            } catch (IOException e) {
                // this really shouldn't happen
                e.printStackTrace();
            }
        }

        // create the tuples
        for (int i = 0; i < tuples.length; i++) {

            // empty slot
            if (!isSlotUsed(i)) {
                for (int j = 0; j < td.getSize(); j++) {
                    try {
                        dos.writeByte(0);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }
                continue;
            }

            // non-empty slot
            for (int j = 0; j < td.numFields(); j++) {
                Field f = tuples[i].getField(j);
                try {
                    f.serialize(dos);

                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        // padding
        int zerolen = BufferPool.getPageSize() - (header.length + td.getSize() * tuples.length); //- numSlots * td.getSize();
        byte[] zeroes = new byte[zerolen];
        try {
            dos.write(zeroes, 0, zerolen);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            dos.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return baos.toByteArray();
    }

    /**
     * Static method to generate a byte array corresponding to an empty
     * HeapPage.
     * Used to add new, empty pages to the file. Passing the results of
     * this method to the HeapPage constructor will create a HeapPage with
     * no valid tuples in it.
     *
     * @return The returned ByteArray.
     */
    public static byte[] createEmptyPageData() {
        int len = BufferPool.getPageSize();
        return new byte[len]; //all 0
    }

    /**
     * Delete the specified tuple from the page; the corresponding header bit should be updated to reflect
     * that it is no longer stored on any page.
     *
     * @param t The tuple to delete
     * @throws DbException if this tuple is not on this page, or tuple slot is
     *                     already empty.
     */
    public void deleteTuple(Tuple t) throws DbException {
        // some code goes here
        // 先找到一个行存放的位置
        final RecordId recordId = t.getRecordId();
        // 找到属于哪个页
        final HeapPageId pageId = (HeapPageId) recordId.getPageId();
        // 存放在这个页中的哪个位置
        final int tn = recordId.getTupleNumber();
        // 如果不是在这个页，返回
        if (!pageId.equals(this.pid)) {
            throw new DbException("Page id not match");
        }
        // 如果这个槽没有被使用，说明出现异常
        if (!isSlotUsed(tn)) {
            throw new DbException("Slot is not used");
        }
        // 将这个槽标记为未使用
        markSlotUsed(tn, false);
        this.tuples[tn] = null;
    }

    /**
     * Adds the specified tuple to the page;  the tuple should be updated to reflect
     * that it is now stored on this page.
     *
     * @param t The tuple to add.
     * @throws DbException if the page is full (no empty slots) or tupledesc
     *                     is mismatch.
     */
    // 都是先写磁盘，再写内存
    public void insertTuple(Tuple t) throws DbException {
        // some code goes here
        // 如果这个行的结构和表的结构不相符，则抛出错误
        if (!t.getTupleDesc().equals(this.td)) {
            throw new DbException("Tuple desc is not match");
        }
        // 遍历这个页，寻找已经不用的槽（行）
        for (int i = 0; i < getNumTuples(); i++) {
            // 如果找到一个已经不使用的槽
            if (!isSlotUsed(i)) {
                // 先将这个槽标记为已使用
                markSlotUsed(i, true);
                // 然后将这个槽分配给这个行
                t.setRecordId(new RecordId(this.pid, i));
                this.tuples[i] = t;
                return;
            }
        }
        // 如果没找到的话，说明这个页已经满了
        throw new DbException("The page is full");
    }

    /**
     * Marks this page as dirty/not dirty and record that transaction
     * that did the dirtying
     */
    public void markDirty(boolean dirty, TransactionId tid) {
        // some code goes here
        // not necessary for lab1
        this.tid = dirty ? tid : null;
    }

    /**
     * Returns the tid of the transaction that last dirtied this page, or null if the page is not dirty
     */
    public TransactionId isDirty() {
        // some code goes here
        // Not necessary for lab1
        return this.tid;
    }

    /**
     * Returns the number of empty slots on this page.
     */
    public int getNumEmptySlots() {
        // some code goes here
        int emptyNum = 0;
        for (int i = 0; i < getNumTuples(); i++) {
            // 位运算，查询效率高
            if (!isSlotUsed(i)) {
                emptyNum++;
            }
        }
        return emptyNum;
    }

    /**
     * Returns true if associated slot on this page is filled.
     */
    // TODO
    public boolean isSlotUsed(int i) {
        // some code goes here
        // For Example, byte = 11110111 and posIndex = 3 -> we want 0
        // 在第几个字节
        int byteIndex = i / 8;
        // 在字节中的第几个位置
        int posIndex = i % 8;
        // 拿到那一个字节
        byte target = this.header[byteIndex];
        //
        return (byte) (target << (7 - posIndex)) < 0;
    }

    /**
     * Abstraction to fill or clear a slot on this page.
     */
    private void markSlotUsed(int i, boolean value) {
        // some code goes here
        // not necessary for lab1
        // 找到属于哪个字节
        int byteIndex = i / 8;
        // 找到字节中的位置
        int posIndex = i % 8;
        byte v = (byte) (1 << posIndex);
        byte headByte = this.header[byteIndex];
        this.header[byteIndex] = value ? (byte) (headByte | v) : (byte) (headByte & ~v);
    }

    /**
     * @return an iterator over all tuples on this page (calling remove on this iterator throws an UnsupportedOperationException)
     * (note that this iterator shouldn't return tuples in empty slots!)
     */
    public Iterator<Tuple> iterator() {
        // some code goes here
        return new IteratorWrapper<>(this.tuples);
    }
}
