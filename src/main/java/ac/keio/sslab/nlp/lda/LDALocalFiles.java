package ac.keio.sslab.nlp.lda;

import java.io.File;

public class LDALocalFiles {
	public final File ldaRootFile;
	public final File sparseFile;
	public final File tfFile;
	public final File dictionaryFile;
	public final File rowIdFile;
	public final File matrixFile;
	public final File docIndexFile;
	public final File cvbFile;
	public final File topicFile;
	public final File documentFile;

	public LDALocalFiles(File LocalLdaRootFile) {
		this.ldaRootFile = LocalLdaRootFile;
		sparseFile = new File(LocalLdaRootFile, LDAFiles.sparseDirName);
		tfFile = new File(LocalLdaRootFile, LDAFiles.tfDirName);
		dictionaryFile = new File(LocalLdaRootFile, LDAFiles.dictionaryFileName);
		rowIdFile = new File(LocalLdaRootFile, LDAFiles.rowIdDirName);
		matrixFile = new File(LocalLdaRootFile, LDAFiles.matrixDirName);
		docIndexFile = new File(LocalLdaRootFile, LDAFiles.docIndexDirName);
		cvbFile = new File(LocalLdaRootFile, LDAFiles.cvbDirName);
		topicFile = new File(LocalLdaRootFile, LDAFiles.topicDirName);
		documentFile = new File(LocalLdaRootFile, LDAFiles.documentDirName);
	}
}
