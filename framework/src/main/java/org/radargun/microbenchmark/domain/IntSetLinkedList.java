package org.radargun.microbenchmark.domain;

import java.io.Serializable;
import java.util.UUID;

import org.radargun.CacheWrapper;


public class IntSetLinkedList implements IntSet, Serializable {

    public class Node implements Serializable {
        /* final */ private int m_value;
        private String uuid;

        public Node(CacheWrapper wrapper, int value, Node next) {
            m_value = value;
            this.uuid = UUID.randomUUID().toString();
            setNext(wrapper, next);
        }

        public Node(CacheWrapper wrapper, int value) {
            this(wrapper, value, null);
        }

        public int getValue() {
            return m_value;
        }

        public void setNext(CacheWrapper wrapper, Node next) {
            if (next != null) {
                Micro.put(wrapper, m_value + ":" + uuid + ":next", next);
            }
        }

        public Node getNext(CacheWrapper wrapper) {
            return (Node) Micro.get(wrapper, m_value + ":" + uuid + ":next");
        }
    }

    /* final */ private Node m_first;

    public IntSetLinkedList() { }

    public IntSetLinkedList(CacheWrapper wrapper) {
        Node min = new Node(wrapper, Integer.MIN_VALUE);
        Node max = new Node(wrapper, Integer.MAX_VALUE);
        min.setNext(wrapper, max);
        m_first = min;
    }

    public boolean add(CacheWrapper wrapper, int value) {
        boolean result;

        Node previous = m_first;
        Node next = previous.getNext(wrapper);
        int v;
        while ((v = next.getValue()) < value) {
            previous = next;
            next = previous.getNext(wrapper);
        }
        result = v != value;
        if (result) {
            previous.setNext(wrapper, new Node(wrapper, value, next));
        }
        
        return result;
    }

    public boolean remove(CacheWrapper wrapper, final int value) {
        boolean result;

        Node previous = m_first;
        Node next = previous.getNext(wrapper);
        int v;
        while ((v = next.getValue()) < value) {
            previous = next;
            next = previous.getNext(wrapper);
        }
        result = v == value;
        if (result) {
            previous.setNext(wrapper, next.getNext(wrapper));
        }

        return result;
    }

    public boolean contains(CacheWrapper wrapper, final int value) {
        boolean result;

        Node previous = m_first;
        Node next = previous.getNext(wrapper);
        int v;
        while ((v = next.getValue()) < value) {
            previous = next;
            next = previous.getNext(wrapper);
        }
        result = (v == value);

        return result;
    }
}
