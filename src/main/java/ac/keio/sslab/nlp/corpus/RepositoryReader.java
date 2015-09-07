package ac.keio.sslab.nlp.corpus;

import java.util.Set;

public interface RepositoryReader {
	public boolean seekNext() throws Exception;
	public String getID();
	public String getDoc();
	public void close() throws Exception;
	public String getStats();
	public String getDate();
	public String getVersion();
	public Set<String> getFiles();
}
