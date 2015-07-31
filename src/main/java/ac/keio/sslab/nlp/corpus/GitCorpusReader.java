package ac.keio.sslab.nlp.corpus;

import java.io.IOException;

public interface GitCorpusReader {
	public boolean seekNext() throws IOException;
	public String getSha();
	public String getDoc();
	public void close() throws IOException;
	public String getStats();
}
