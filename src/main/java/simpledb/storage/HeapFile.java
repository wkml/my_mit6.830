package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import simpledb.util.HeapFileIterator;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private final File file;
    private final TupleDesc td;
    private RandomAccessFile randomAccessFile;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.td = td;
        try {
            this.randomAccessFile = new RandomAccessFile(this.file, "rw");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return this.file.getAbsolutePath().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 获取某一个文件的表头信息
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        //
        final int pos = BufferPool.getPageSize() * pid.getPageNumber();
        byte[] pageData = new byte[BufferPool.getPageSize()];
        try {
            this.randomAccessFile.seek(pos);
            this.randomAccessFile.read(pageData, 0, pageData.length);
            return new HeapPage((HeapPageId) pid, pageData);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    // see DbFile.java for javadocs

    /**
     * 将一个页写入到文件中
     * @param page 要写入到文件的页
     * @throws IOException 异常
     */
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        final int pos = BufferPool.getPageSize() * page.getId().getPageNumber();
        this.randomAccessFile.seek(pos);
        final byte[] pageData = page.getPageData();
        this.randomAccessFile.write(pageData);
    }

    /**
     * Returns the number of pages in this HeapFile.
     * 这个file一共放在了几个页中
     */
    public int numPages() {
        // some code goes here
        // 向上取整
        // TODO 向上取整，可能会出问题
        return (int) Math.ceil(this.file.length() * 1.0 / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    // 事务将指定的元组插入文件。
    // 此方法将在文件的受影响页面上获取锁，并且可能会阻塞，直到可以获取锁为止。
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t) throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // 脏页列表，说明这个页在内存中已经修改，但是还没有写入到磁盘
        final ArrayList<Page> dirtyPageList = new ArrayList<>();
        // 遍历bufferPool中的页，寻找空槽
        for (int i = 0; i < this.numPages(); i++) {
            final HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), i), Permissions.READ_WRITE);
            // 一个页是随机读取的，只要文件中的任意一个页，并且那个页中有空槽，就可以将一条记录（行）插入到那个槽中
            if (page != null && page.getNumEmptySlots() > 0) {
                page.insertTuple(t);
                // 标记这个页是脏页，后面得刷到文件中
                page.markDirty(true, tid);
                dirtyPageList.add(page);
                break;
            }
        }
        // That means all pages are full, we should create a new page.
        // 走到这个分支，说明现在这个文件占有的所有页都已经满了，要新增一个页
        if (dirtyPageList.size() == 0) {
            // 新建一个空页，页的编号是现在的页的数量（因为从0开始）
            final HeapPageId heapPageId = new HeapPageId(getId(), this.numPages());
            HeapPage newPage = new HeapPage(heapPageId, HeapPage.createEmptyPageData());
            // 先把这个空页写到磁盘中
            writePage(newPage);
            // Through buffer pool to get newPage
            // 这一步会先从缓存中拿，再从文件中拿，所以一定能拿到，并且会加入到缓存中
            newPage = (HeapPage) Database.getBufferPool().getPage(tid, heapPageId, Permissions.READ_WRITE);
            // 将这条记录写入内存页，并标记为脏页
            newPage.insertTuple(t);
            newPage.markDirty(true, tid);
            dirtyPageList.add(newPage);
        }
        return dirtyPageList;
    }

    // see DbFile.java for javadocs
    // 指定事务从文件中删除指定元组。
    // 此方法将在文件的受影响页面上获取锁，并且可能会阻塞，直到可以获取锁为止。
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException, TransactionAbortedException {
        // some code goes here
        final ArrayList<Page> dirtyPageList = new ArrayList<>();
        // 获取这条记录的位置
        final RecordId recordId = t.getRecordId();
        final PageId pageId = recordId.getPageId();
        // 获取所在的页
        final HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
        // 在该页上删除该记录
        if (page != null && page.isSlotUsed(recordId.getTupleNumber())) {
            page.deleteTuple(t);
            // 标记脏页，准备刷脏
            dirtyPageList.add(page);
        }
        return dirtyPageList;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(numPages(), tid, this.getId());
    }
}
