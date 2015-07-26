package ac.keio.sslab.nlp.corpus;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

public class DocumentFilter {
	StringBuilder filtered = new StringBuilder();
	boolean tokenizeAtUnderline, useNLTKStopwords;

	public DocumentFilter(boolean tokenizeAtUnderline, boolean useNLTKStopwords) {
		this.tokenizeAtUnderline = tokenizeAtUnderline;
		this.useNLTKStopwords = useNLTKStopwords;
	}

	public String filterParagraph(String paragraph) throws IOException {
		Analyzer wordAnalyzer = new WordAnalyzer(tokenizeAtUnderline, useNLTKStopwords);
		filtered.setLength(0);
		TokenStream stream = wordAnalyzer.tokenStream("", new StringReader(paragraph));
		CharTermAttribute term = stream.getAttribute(CharTermAttribute.class);
		stream.reset();

		while (stream.incrementToken()) {
			String word = term.toString();
			filtered.append(word);
			filtered.append(' ');
		}
		stream.end();
		stream.close();
		if (filtered.length() >= 1) {
			// strip the last delimiter
			filtered.setLength(filtered.length() - 1);
		}
		wordAnalyzer.close();
		return filtered.toString();
	}

	public List<String> filterDocument(String document) throws IOException {
		Analyzer paragraphAnalyzer = new ParagraphAnalyzer();
		List<String> ret = new ArrayList<String>();
		TokenStream stream = paragraphAnalyzer.tokenStream("", new StringReader(document));
		CharTermAttribute term = stream.getAttribute(CharTermAttribute.class);
		stream.reset();

		while (stream.incrementToken()) {
			String paragraph = term.toString();
			ret.add(filterParagraph(paragraph));
		}
		stream.end();
		stream.close();
		paragraphAnalyzer.close();
		return ret;
	}
}
