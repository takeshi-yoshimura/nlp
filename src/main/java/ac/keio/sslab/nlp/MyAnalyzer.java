package ac.keio.sslab.nlp;

import java.io.Reader;
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.StopAnalyzer;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.en.KStemFilter;
import org.apache.lucene.analysis.miscellaneous.LengthFilter;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.util.Version;

class MyAnalyzer extends Analyzer {

	protected boolean tokenizeAtUnderline;
	protected static final List<String> NLTKstopwordList = Arrays.asList("i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your",
			"yours", "yourself", "yourselves", "he", "him", "his", "himself", "she",
			"her", "hers", "herself", "it", "its", "itself", "they", "them", "their",
			"theirs", "themselves", "what", "which", "who", "whom", "this", "that",
			"these", "those", "am", "is", "are", "was", "were", "be", "been", "being",
			"have", "has", "had", "having", "do", "does", "did", "doing", "a", "an",
			"the", "and", "but", "if", "or", "because", "as", "until", "while", "of",
			"at", "by", "for", "with", "about", "against", "between", "into", "through",
			"during", "before", "after", "above", "below", "to", "from", "up", "down",
			"in", "out", "on", "off", "over", "under", "again", "further", "then", "once",
			"here", "there", "when", "where", "why", "how", "all", "any", "both", "each",
			"few", "more", "most", "other", "some", "such", "no", "nor", "not", "only",
			"own", "same", "so", "than", "too", "very", "s", "t", "can", "will", "just",
			"don", "should", "now");

	protected CharArraySet stopwords;

	public MyAnalyzer(boolean tokenizeAtUnderline, boolean useNLTKStopwords) {
		this.tokenizeAtUnderline = tokenizeAtUnderline;
		stopwords = useNLTKStopwords ? new CharArraySet(Version.LUCENE_46, NLTKstopwordList, false): StopAnalyzer.ENGLISH_STOP_WORDS_SET;
	}

	@Override
	protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
		Tokenizer tokenizer = new MyTokenizer(Version.LUCENE_46, reader, tokenizeAtUnderline);
		TokenStream filter = new LengthFilter(Version.LUCENE_46, tokenizer, 2, 20);

		filter = new LowerCaseFilter(Version.LUCENE_46, filter);
		filter = new StopFilter(Version.LUCENE_46, filter, stopwords);
		filter = new KStemFilter(filter);
		return new TokenStreamComponents(tokenizer, filter);
	}
}