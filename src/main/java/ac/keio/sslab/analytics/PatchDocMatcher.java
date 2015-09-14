package ac.keio.sslab.analytics;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PatchDocMatcher {

	Map<String, String> regex;

	public PatchDocMatcher(File regDocFile) throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(regDocFile));
		regex = new HashMap<>();
		String line = null;
		while ((line = br.readLine()) != null) {
			if (line.matches("^\\s*#.*$")) {
				continue;
			}
			Pattern p = Pattern.compile("^\"(.*)\",\"(.*)\"$");
			Matcher m = p.matcher(line);
			if (!m.find() || m.groupCount() != 2) {
				System.out.println("Ignore broken line: " + line);
				continue;
			}
			System.out.println(line);
			regex.put(m.group(1), m.group(2));
		}
		br.close();
	}

	public PatchDocMatcher(Map<String, String> regex) {
		this.regex = regex;
	}

	public Map<String, List<String>> matchedGroup(String str) {
		Map<String, List<String>> ret = new HashMap<>();
		String str2 = str.toLowerCase();
		for (Entry<String, String> e: regex.entrySet()) {
			Pattern p = Pattern.compile(e.getValue());
			Matcher m = p.matcher(str2);
			while (m.find()) {
				if (!ret.containsKey(e.getKey())) {
					ret.put(e.getKey(), new ArrayList<String>());
				}
				ret.get(e.getKey()).add(m.group());
			}
		}
		return ret;
	}

	public List<String> match(String str) {
		List<String> ret = new ArrayList<>();
		String str2 = str.toLowerCase();
		for (Entry<String, String> e: regex.entrySet()) {
			Pattern p = Pattern.compile(e.getValue());
			Matcher m = p.matcher(str2);
			if (m.find()) {
				ret.add(e.getKey());
			}
		}
		return ret;
	}

	public Set<String> keySet() {
		return regex.keySet();
	}
}
