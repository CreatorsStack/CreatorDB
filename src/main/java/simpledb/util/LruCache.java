package simpledb.util;

import java.util.*;
import java.util.stream.Collectors;

public class LruCache<K, V> {

    // LruCache node
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
        Node next = this.head.next;
        node.next = next;
        node.pre = this.head;

        this.head.next = node;
        next.pre = node;
    }

    public void moveToHead(Node node) {
        removeNode(node);
        linkToHead(node);
    }

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

    public void remove(K key) {
        if (this.nodeMap.containsKey(key)) {
            final Node node = this.nodeMap.get(key);
            removeNode(node);
            this.nodeMap.remove(key);
        }
    }

    public V get(K key) {
        if (this.nodeMap.containsKey(key)) {
            Node node = this.nodeMap.get(key);
            moveToHead(node);
            return node.value;
        }
        return null;
    }

    // Return the evicted item if the space is insufficient
    public V put(K key, V value) {
        if (this.nodeMap.containsKey(key)) {
            Node node = this.nodeMap.get(key);
            node.value = value;
            moveToHead(node);
        } else {
            if (this.nodeMap.size() == this.maxSize) {
                Node last = removeLast();
                this.nodeMap.remove(last.key);
                return last.value;
            }
            Node node = new Node(key, value);
            this.nodeMap.put(key, node);
            linkToHead(node);
        }
        return null;
    }

    public Iterator<V> valueIterator() {
        final Collection<Node> nodes = this.nodeMap.values();
        final List<V> valueList = nodes.stream().map(x -> x.value).collect(Collectors.toList());
        return valueList.iterator();
    }
}
