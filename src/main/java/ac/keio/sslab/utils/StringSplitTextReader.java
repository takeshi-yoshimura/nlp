package ac.keio.sslab.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class StringSplitTextReader {

	BufferedReader br;
	String [] next;
	int nextIndex;
	String regex;

	public StringSplitTextReader(File f, String regex) throws IOException {
		br = new BufferedReader(new FileReader(f));
		next = new String [] {""}; //dummy
		nextIndex = 0;
		this.regex = regex;
	}

	public boolean seekNext() throws IOException {
		if (++nextIndex == next.length) {
			String line = br.readLine();
			if (line == null) {
				return false;
			}
			while (line.matches("^\\s*#.*$")) { // skip comment lines
				line = br.readLine();
				if (line == null) {
					return false;
				}
			}
			next = line.split(regex);
			nextIndex = 0;
		}
		return true;
	}

	public boolean isEndOfLine() throws IOException {
		return (nextIndex + 1) == next.length;
	}

	public String next() {
		return next[nextIndex];
	}

	public void close() throws IOException {
		br.close();
	}
}
