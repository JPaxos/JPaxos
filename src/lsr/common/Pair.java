package lsr.common;

import java.io.Serializable;

/**
 * A Pair is an object that contains two other objects.
 */
public class Pair<K, V> implements Serializable {

    // I have no idea how and why:
    // 1) transient will work for serial version
    // 2) neither eclipse nor ant throws warning about senseless serial
    private transient static final long serialVersionUID = 1L;
    private K key;
    private V value;

    public Pair(K key, V value) {
        this.key = key;
        this.value = value;
    }

    public void setKey(K a) {
        this.key = a;
    }

    public K getKey() {
        return key;
    }

    public K key() {
        return key;
    }

    public void setValue(V value) {
        this.value = value;
    }

    public V getValue() {
        return value;
    }

    public V value() {
        return value;
    }

    public String toString() {
        return "<" + key.toString() + "; " + value.toString() + ">";
    }

    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        Pair<?, ?> other = (Pair<?, ?>) obj;
        if (key == null) {
            if (other.key != null) {
                return false;
            }
        } else if (!key.equals(other.key)) {
            return false;
        }
        if (value == null) {
            if (other.value != null) {
                return false;
            }
        } else if (!value.equals(other.value)) {
            return false;
        }
        return true;
    }

    public int hashCode() {
        int hash = 7;
        hash = 31 * hash + (key == null ? 0 : key.hashCode());
        hash = 31 * hash + (value == null ? 0 : value.hashCode());
        return hash;
    }
};