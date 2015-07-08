package ac.keio.sslab.nlp;

import java.io.Reader;
import java.util.Arrays;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.StopAnalyzer;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.en.KStemFilter;
import org.apache.lucene.analysis.miscellaneous.LengthFilter;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.util.Version;



class MyAnalyzer extends Analyzer {
	CharArraySet stopWords = StopFilter.makeStopSet(Version.LUCENE_46, Arrays.asList(StopAnalyzer.ENGLISH_STOP_WORDS_SET), true);
	@Override
	protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
		Tokenizer source = new MyTokenizer(Version.LUCENE_46, reader);
		TokenStream result = new LengthFilter(Version.LUCENE_46, source, 3, 20);
		result = new StopFilter(Version.LUCENE_46, new KStemFilter(result), stopWords);
		return new TokenStreamComponents(source, result);
	}
}