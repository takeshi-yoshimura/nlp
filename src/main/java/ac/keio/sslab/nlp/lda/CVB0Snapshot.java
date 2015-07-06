package ac.keio.sslab.nlp.lda;

import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.GlobFilter;
import org.apache.hadoop.fs.Path;

import ac.keio.sslab.nlp.JobUtils;

public class CVB0Snapshot extends LDASnapshotJob {

	public CVB0Snapshot(FileSystem fs, LDAHDFSFiles hdfs, LDALocalFiles local) {
		super(fs, hdfs, local);
	}

	@Override
	public void takeSnapshot() {
		//Check if the directory layout is correct
		Map<Path, String> checkMap = new HashMap<Path, String>();
		checkMap.put(hdfs.modelPath,  "CVB model directory");
		if (!pathsExist(checkMap)) {
			return;
		}

		//List up files to be copied
		Set<Path> copySet = new HashSet<>();
		copySet.add(hdfs.topicPath);
		copySet.add(hdfs.documentPath);
		try {
			for (FileStatus stat: fs.listStatus(hdfs.modelPath, new GlobFilter("perplexity-[0-9]+"))) {
				if (JobUtils.isSucceededMapReduce(fs, stat.getPath())) {
					copySet.add(stat.getPath());
				}
			}
			//Copy only the last succeeded file to save the disk space and snapshot time
			//Mahout LDA does not require model-XX/part-r-YYY files except for the last model-XX to restart aborted iterations
			List<Integer> models = new ArrayList<Integer>();
			for (FileStatus stat: fs.listStatus(hdfs.modelPath, new GlobFilter("model-[0-9]+"))) {
				if (JobUtils.isSucceededMapReduce(fs, stat.getPath())) {
					models.add(Integer.parseInt(stat.getPath().getName().substring("model-".length())));
				}
			}
			Collections.sort(models, Collections.reverseOrder());
			for (Integer model: models) {
				Path mp = new Path(hdfs.modelPath, "model-" + model.toString());
				if (JobUtils.isSucceededMapReduce(fs, mp)) {
					copySet.add(mp);
					break;
				}
			}
		} catch (Exception e) {
			System.err.println("Taking the snapshot of cvb aborts: " + e.toString());
			System.err.println("Checking HDFS states and the consistency of directory " + hdfs.modelPath);
			return;
		}

		//Copy the files from HDFS to a local disk
		copyFromHDFSToLocalFS(copySet);
	}

	/**
	 * Reproduce the correct directory layout on HDFS for Mahout LDA while ensuring the exception safety.
	 * In addition, create dummy cvb/model/model-XX directories in a temporary directory in HDFS
	 * This method is called by CopyFromLocalFSToHDFS().
	 */
	@Override
	protected void hookCopyFromLocalFSToHDFS() throws Exception {
		long modTime = fs.getFileStatus(tmpPath).getModificationTime();
		FileStatus [] stat = fs.listStatus(tmpPath, new GlobFilter("model-[0-9]+"));
		if (stat.length != 1) {
			throw new Exception(tmpPath + " has more than 2 cvb/model-XX directories.");
		}

		Path modelPath = new Path(tmpPath.getParent(), LDAFiles.modelDirName);
		Path modelOne = stat[0].getPath();
		int numModels = Integer.parseInt(modelOne.getName().substring("model-".length()));
		long modelOneModTime = fs.getFileStatus(modelOne).getModificationTime();
		Path newModelOne = new Path(modelPath, modelOne.getName());

		//Create cvb/model and cvb/model/model-XX
		fs.mkdirs(modelPath);
		fs.setTimes(tmpPath, modTime, -1);
		fs.rename(modelOne, newModelOne);
		fs.setTimes(newModelOne, modelOneModTime, -1);
		for (int i = 1; i < numModels; i++) {
			fs.mkdirs(new Path(modelPath, "model-" + i));
		}

		//Move cvb/perplexity-XX to cvb/model/
		for (FileStatus stat2: fs.listStatus(tmpPath, new GlobFilter("perplexity-[0-9]+"))) {
			long perplexityModTime = fs.getFileStatus(stat2.getPath()).getModificationTime();
			Path newPerplexityPath = new Path(modelPath, stat2.getPath().getName());
			fs.rename(stat2.getPath(), newPerplexityPath);
			fs.setTimes(newPerplexityPath, perplexityModTime, -1);
		}
		fs.setTimes(modelPath, modelOneModTime, -1);
	}

	@Override
	public void restoreSnapshot() {
		//Check the snapshot in a local disk
		Map<File, String> checkMap = new HashMap<File, String>();
		if (!filesExist(checkMap)) {
			return;
		}

		File modelFile;
		File [] perplexityFiles;
		try {
			File [] modelFiles = local.cvbFile.listFiles(new FilenameFilter() {
				public boolean accept(File dir, String name) {
					return name.matches("model-[0-9]+");
				}
			});
			if (modelFiles.length == 0) {
				System.err.println("Not found: cvb/model-XX. Taking a snapshot is meaningless. Aborts!");
				return;
			} else if (modelFiles.length != 1) {
				System.err.println("There are more than 2 cvb/model-XX in " + localOutputFile.getAbsolutePath() + ". Aborts!");
				return;
			}
			modelFile = modelFiles[0];
			perplexityFiles = local.cvbFile.listFiles(new FilenameFilter() {
				public boolean accept(File dir, String name) {
					return name.matches("perplexity-[0-9]+");
				}
			});
		} catch (NumberFormatException e) {
			System.err.println("Restoring the snapshot of cvb aborts: " + e.toString());
			System.err.println("XX in " + local.cvbFile + "/model-XX must be an integer");
			System.err.println("Try deleting or fixing the name of direcories in " + local.cvbFile);
			return;
		} catch (Exception e) {
			System.err.println("Reading or Deleting directories in a local disk failed: " + e.toString());
			System.err.println("Check permissions: " + local.cvbFile);
			return;
		}

		//Copy the files from a local disk to HDFS
		Set<File> set = new HashSet<File>();
		set.add(local.topicFile);
		set.add(local.documentFile);
		set.add(modelFile);
		for (File file: perplexityFiles) {
			set.add(file);
		}
		copyFromLocalFSToHDFS(set); //calls hookCopyFromLocalFSToHDFS()
	}

	@Override
	protected String getJobName() {
		return LDAFiles.cvbJobName;
	}

}
