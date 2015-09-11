package ac.keio.sslab.utils;

import java.util.Comparator;
import java.util.Map.Entry;

public class NumMapReverseOrder<T, N extends Comparable<N>> implements Comparator<Entry<T, N>> {

	@Override
	public int compare(Entry<T, N> entry1, Entry<T, N> entry2) {
		return entry2.getValue().compareTo(entry1.getValue());
	}
}