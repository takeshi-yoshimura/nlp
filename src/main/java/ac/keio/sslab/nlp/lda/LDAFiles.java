package ac.keio.sslab.nlp.lda;

import ac.keio.sslab.nlp.NLPConf;

public class LDAFiles {
	public static final String sparseJobName = "sparse";
	public static final String rowIdJobName = "rowid";
	public static final String cvbJobName = "cvb";

	public static final String argumentFileName = "argument";
	public static final String mapReduceSuccessFileName = "_SUCCESS";
	public static final String swapFileName = "swap";
	public static final String sparseDirName = sparseJobName;
	public static final String tfDirName = sparseDirName + "/tf-vectors";
	public static final String dictionaryFileName = sparseDirName + "/dictionary.file-0";
	public static final String rowIdDirName = rowIdJobName;
	public static final String matrixDirName = rowIdDirName + "/matrix";
	public static final String docIndexDirName = rowIdDirName + "/docIndex";
	public static final String cvbDirName = cvbJobName;
	public static final String topicDirName = cvbDirName + "/topic";
	public static final String documentDirName = cvbDirName + "/document";
	public static final String modelDirName = cvbDirName + "/model";
	public static final String tmpDirName = NLPConf.tmpDirName;
	public static final String splitMatrixDirName = rowIdJobName + "/splitMatrix";
}
