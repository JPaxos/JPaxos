package lsr.common;

import java.io.Serializable;

/**
 * Presenting one of the most typical data structures unavailable to Java. Until
 * now!
 * 
 * The hardly possible to implement structure, probably the most used data
 * structure of the world, now may be taken for 0,00CHF only!
 * 
 * The pair, but you already know that from the first words.
 * 
 * <advertisment end>
 * 
 * There is no pair in Java? Hey, stop kidding me!
 * 
 * Wait, what means Your'e serious?
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

	public Pair<K, V> clone() {
		return new Pair<K, V>(key, value);
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

	@SuppressWarnings("unchecked")
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Pair other = (Pair) obj;
		if (key == null) {
			if (other.key != null)
				return false;
		} else if (!key.equals(other.key))
			return false;
		if (value == null) {
			if (other.value != null)
				return false;
		} else if (!value.equals(other.value))
			return false;
		return true;
	}
};