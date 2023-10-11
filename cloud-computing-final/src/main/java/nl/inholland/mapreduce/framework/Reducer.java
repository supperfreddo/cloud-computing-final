package nl.inholland.mapreduce.framework;

import java.util.List;

public interface Reducer<K, V> {
	V reduce(K key, List<V> values);
}