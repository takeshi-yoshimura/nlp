package ac.keio.sslab.nlp;

import java.io.Reader;

import org.apache.lucene.analysis.util.CharTokenizer;
import org.apache.lucene.util.Version;


class MyTokenizer extends CharTokenizer {

	protected boolean tokenizeAtUnderline;

	public MyTokenizer(Version matchVersion, Reader in, boolean tokenizeAtUnderline) {
		super(matchVersion, in);
		this.tokenizeAtUnderline = tokenizeAtUnderline;
	}

	@Override
	protected boolean isTokenChar(int c) {
		return Character.isAlphabetic(c) || Character.isDigit(c) || (tokenizeAtUnderline && c == '_');
	}
}
