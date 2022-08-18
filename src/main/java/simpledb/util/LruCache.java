package simpledb.util;

import java.util.*;
import java.util.stream.Collectors;

public class LruCache<K, V> {

    // LruCache node
    // 采用链表实现
    public class Node {
        public Node pre;
        public Node next;
        public K    key;
        public V    value;

        public Node(final K key, final V value) {
            this.key = key;
            this.value = value;
        }
    }

    private final int          maxSize;
    private final Map<K, Node> nodeMap;
    private final Node         head;
    private final Node         tail;

    public LruCache(int maxSize) {
        this.maxSize = maxSize;
        this.head = new Node(null, null);
        this.tail = new Node(null, null);
        this.head.next = tail;
        this.tail.pre = head;
        this.nodeMap = new HashMap<>();
    }

    public void linkToHead(Node node) {
        // 拿到第一个
        Node next = this.head.next;
        // 将现在的节点插入到第一个之前
        node.next = next;
        // 前置指针指向头
        node.pre = this.head;

        this.head.next = node;
        next.pre = node;
    }

    // 将一个node移动到头部
    public void moveToHead(Node node) {
        // 删除
        removeNode(node);
        // 新增
        linkToHead(node);
    }

    // 已经拿到链表中的一个节点，将这个节点删除
    // 双向链表移除
    public void removeNode(Node node) {
        if (node.pre != null && node.next != null) {
            node.pre.next = node.next;
            node.next.pre = node.pre;
        }
    }

    public Node removeLast() {
        Node last = this.tail.pre;
        removeNode(last);
        return last;
    }

    // 移除某个指定key的节点
    public synchronized void remove(K key) {
        if (this.nodeMap.containsKey(key)) {
            final Node node = this.nodeMap.get(key);
            removeNode(node);
            this.nodeMap.remove(key);
        }
    }

    public synchronized V get(K key) {
        if (this.nodeMap.containsKey(key)) {
            Node node = this.nodeMap.get(key);
            moveToHead(node);
            return node.value;
        }
        return null;
    }

    // Return the evicted item if the space is insufficient
    public synchronized V put(K key, V value) {
        // 如果存在的话，替换node原来的值，并移动到头
        if (this.nodeMap.containsKey(key)) {
            Node node = this.nodeMap.get(key);
            node.value = value;
            moveToHead(node);
        } else {
            // 如果不存在，创建新的node，并加入到表的头部
            // TODO 是否需要考虑cache满的情况

            //            if (this.nodeMap.size() == this.maxSize) {
            //                Node last = removeLast();
            //                this.nodeMap.remove(last.key);
            //                return last.value;
            //            }
            Node node = new Node(key, value);
            this.nodeMap.put(key, node);
            linkToHead(node);
        }
        return null;
    }

    public synchronized Iterator<V> reverseIterator() {
        Node last = this.tail.pre;
        final ArrayList<V> list = new ArrayList<>();
        while (!last.equals(this.head)) {
            list.add(last.value);
            last = last.pre;
        }
        return list.iterator();
    }

    public synchronized Iterator<V> valueIterator() {
        final Collection<Node> nodes = this.nodeMap.values();
        final List<V> valueList = nodes.stream().map(x -> x.value).collect(Collectors.toList());
        return valueList.iterator();
    }

    public synchronized int getSize() {
        return this.nodeMap.size();
    }

    public int getMaxSize() {
        return maxSize;
    }
}
