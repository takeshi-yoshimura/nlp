package ac.keio.sslab.utils;

import java.util.Iterator;
import java.util.TreeSet;

import org.json.JSONObject;

public class OrderedJson extends JSONObject {
	@SuppressWarnings("rawtypes")
	@Override
	public Iterator keys() {
		TreeSet<Object> sortedKeys = new TreeSet<Object>();
		Iterator keys = super.keys();
		while (keys.hasNext()) {
			sortedKeys.add(keys.next());
		}
		return sortedKeys.iterator();
	}
}
