package ac.keio.sslab.utils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.google.common.collect.Lists;

// get/put inside a division are thread-safe
public class ArrayMap<K, V> {

	List<Map<K, List<V>>> map;

	public ArrayMap(int capacity) {
		map = Lists.newArrayListWithCapacity(capacity);
		for (int n = 0; n < capacity; n++) {
			map.add(new HashMap<K, List<V>>());
		}
	}

	public static <K, V> ArrayMap<K, V> newMap(int capacity) {
		return new ArrayMap<K, V>(capacity);
	}

	public void init(int division, K key) {
		List<V> values = Lists.newArrayList();
		map.get(division).put(key, values);
	}

	public void init(int division, K key, V value) {
		init(division, key);
		put(division, key, value);
	}

	public void put(int division, K key, V value) {
		map.get(division).get(key).add(value);
	}

	public int findDivision(K key) {
		int n = map.size() - 1;
		while (n >= 0 && !map.get(n).containsKey(key)) {
			--n;
		}
		return n;
	}

	public boolean contains(K key) {
		return findDivision(key) != -1;
	}

	public List<V> remove(int division, K key) {
		return map.get(division).remove(key);
	}

	public List<V> removeKey(K key) {
		return remove(findDivision(key), key);
	}

	public List<V> get(int division, K key) {
		return map.get(division).get(key);
	}

	public List<V> values(K key) {
		return get(findDivision(key), key);
	}

	public Set<K> keySet(int division) {
		return map.get(division).keySet();
	}

	public Set<Entry<K, List<V>>> entrySet(int division) {
		return map.get(division).entrySet();
	}

	public void addAll(int division, K key, List<V> values) {
		map.get(division).get(key).addAll(values);
	}

	public void addAlltoKey(K key, List<V> values) {
		addAll(findDivision(key), key, values);
	}

	public int size() {
		int size = 0;
		for (Map<K, List<V>> e: map) {
			size += e.size();
		}
		return size;
	}

	public int numDivisions() {
		return map.size();
	}

	public boolean contains(int division, int i) {
		return map.get(division).containsKey(i);
	}
}
