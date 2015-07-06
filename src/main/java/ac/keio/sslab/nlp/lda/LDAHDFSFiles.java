package ac.keio.sslab.nlp.lda;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;

public class LDAHDFSFiles {
	public final Path ldaRootPath;
	public final Path sparsePath;
	public final Path tfPath;
	public final Path dictionaryPath;
	public final Path rowIdPath;
	public final Path matrixPath;
	public final Path docIndexPath;
	public final Path cvbPath;
	public final Path topicPath;
	public final Path documentPath;
	public final Path modelPath;
	public final Path splitMatrixPath;
	public final List<String> mapReduceOutputDirNames;

	public LDAHDFSFiles(Path ldaRootPath) {
		this.ldaRootPath = ldaRootPath;
		sparsePath = new Path(ldaRootPath, LDAFiles.sparseDirName);
		tfPath = new Path(ldaRootPath, LDAFiles.tfDirName);
		dictionaryPath = new Path(ldaRootPath, LDAFiles.dictionaryFileName);
		rowIdPath = new Path(ldaRootPath, LDAFiles.rowIdDirName);
		matrixPath = new Path(ldaRootPath, LDAFiles.matrixDirName);
		docIndexPath = new Path(ldaRootPath, LDAFiles.docIndexDirName);
		cvbPath = new Path(ldaRootPath, LDAFiles.cvbDirName);
		topicPath = new Path(ldaRootPath, LDAFiles.topicDirName);
		documentPath = new Path(ldaRootPath, LDAFiles.documentDirName);
		modelPath = new Path(ldaRootPath, LDAFiles.modelDirName);
		splitMatrixPath = new Path(ldaRootPath, LDAFiles.splitMatrixDirName);
		mapReduceOutputDirNames = new ArrayList<String>();
		mapReduceOutputDirNames.add(tfPath.getName());
		mapReduceOutputDirNames.add(topicPath.getName());
		mapReduceOutputDirNames.add(documentPath.getName());
		mapReduceOutputDirNames.add(modelPath.getName());
	}
}
