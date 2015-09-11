package ac.keio.sslab.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class SimpleSorter {

	public static <T, N extends Comparable<N>> List<Entry<T, N>> reverse(Map<T, N> map) {
		List<Entry<T, N>> l = new ArrayList<Entry<T, N>>(map.entrySet());
		Collections.sort(l, new NumMapReverseOrder<T, N>());
		return l;
	}

	public static <N extends Comparable<N>> List<N> reverse(Collection<N> col) {
		List<N> l = new ArrayList<N>(col);
		Collections.sort(l, Collections.reverseOrder());
		return l;
	}

	public static <T, N extends Comparable<N>> List<Entry<T, N>> natural(Map<T, N> map) {
		List<Entry<T, N>> l = new ArrayList<Entry<T, N>>(map.entrySet());
		Collections.sort(l, new NumMapNaturalOrder<T, N>());
		return l;
	}

	public static <N extends Comparable<N>> List<N> natural(Collection<N> col) {
		List<N> l = new ArrayList<N>(col);
		Collections.sort(l);
		return l;
	}
}
