package ac.keio.sslab.nlp;

import java.io.Reader;

import org.apache.lucene.analysis.util.CharTokenizer;
import org.apache.lucene.util.Version;


class MyTokenizer extends CharTokenizer {

	public MyTokenizer(Version matchVersion, Reader in) {
		super(matchVersion, in);
	}

	@Override
	protected boolean isTokenChar(int c) {
		return Character.isAlphabetic(c) || Character.isDigit(c) || c == '_';
	}
}
