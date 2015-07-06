package ac.keio.sslab.nlp.lda;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.eclipse.jgit.util.FileUtils;

import ac.keio.sslab.nlp.JobUtils;

public abstract class LDASnapshotJob {
	public abstract void takeSnapshot();
	public abstract void restoreSnapshot();
	protected abstract String getJobName();

	protected FileSystem fs;
	protected LDAHDFSFiles hdfs;
	protected LDALocalFiles local;
	protected Path outputPath;
	protected File localOutputFile;
	protected Path argPath;
	protected File localArgFile;
	protected Path tmpPath;
	protected File localTmpFile;

	public LDASnapshotJob(FileSystem fs, LDAHDFSFiles hdfs, LDALocalFiles local) {
		this.fs = fs;
		this.hdfs = hdfs;
		this.local = local;
		switch (getJobName()) {
			case LDAFiles.cvbJobName:
				outputPath = hdfs.cvbPath;
				localOutputFile = local.cvbFile;
				break;
			case LDAFiles.rowIdJobName:
				outputPath = hdfs.rowIdPath;
				localOutputFile = local.rowIdFile;
				break;
			case LDAFiles.sparseJobName:
				outputPath = hdfs.sparsePath;
				localOutputFile = local.sparseFile;
				break;
		}
		argPath = new Path(outputPath, LDAFiles.argumentFileName);
		localArgFile = new File(local.ldaRootFile, LDAFiles.argumentFileName);
		tmpPath = new Path(LDAFiles.tmpDirName, outputPath);
		localTmpFile = new File(LDAFiles.tmpDirName, localOutputFile.getAbsolutePath());
	}

	/**
	 * Check if all the directories exists in HDFS
	 * 
	 * @param map keys are Path to be checked and values are description of keys
	 * @return true If all the key paths exists, otherwise false
	 */
	protected boolean pathsExist(Map<Path, String> map) {
		map.put(argPath, "Argument file for " + getJobName());
		map.put(outputPath, "Output directory" + getJobName());
		map.put(hdfs.ldaRootPath, "LDA root directory");
		try {
			List<Path> nonExists = new ArrayList<Path>();
			for (Path path: map.keySet()) {
				if (!fs.exists(path)) {
					nonExists.add(path);
				}
			}
			if (!nonExists.isEmpty()) {
				System.err.println("Some of " + getJobName() + " outputs are not found.");
				for (Map.Entry<Path, String> e: map.entrySet()) {
					System.err.println(e.getValue() + " " + e.getKey() + ": " + (nonExists.contains(e.getKey())? "found" : "not found"));
				}
				return false;
			}
			return true;
		} catch (Exception e) {
			System.err.println("It seems like connecting HDFS failed: " + e.toString());
			System.err.println("Check configurations and network connections. Abort");
			return false;
		}
	}

	/**
	 * Copy the files from HDFS to a local disk.
	 * Use several swapping to ensure the exception/error safety which guarantees the consistency of job directory layout.
	 * Unlike copyFromLocalfsToHDFS(), this method runs frequently. Thus, it tries to avoid deleting the existing directories.
	 * 
	 * @param outputFile Destination directory of the copy.
	 * @param list Copied files on HDFS
	 */
	protected void copyFromHDFSToLocalFS(Set<Path> paths) {
		Set<Path> set = new HashSet<Path>();
		try {
			set.add(argPath);
			for (Path path: paths) {
				if (set.contains(path)) {
					continue;
				}
				//Copy only files which derive from succeeded MapReduce.
				if (hdfs.mapReduceOutputDirNames.contains(path.getName()) && JobUtils.isSucceededMapReduce(fs, path)) {
					set.add(path);
				} else if (!hdfs.mapReduceOutputDirNames.contains(path.getName()) && fs.exists(path)) {
					set.add(path);
				}
			}

			//Check if the past snapshot exists and it is up-to-date.
			if (localOutputFile.exists()) {
				boolean modified = false;
				for (Path path: set) {
					File f = new File(localOutputFile, path.getName());
					if (!f.exists() || f.lastModified() < fs.getFileStatus(path).getModificationTime()) {
						modified = true;
					}
				}
				if (!modified) {
					System.out.println("Files are up-to-date. Skip taking the snapshot of " + getJobName());
					return;
				}
				System.out.println("Files for " + getJobName() + " should be updated. Delete and overwrite files.");
			}
		} catch (Exception e) {
			System.err.println("Taking the snapshot of " + getJobName() + " aborts: " + e.toString());
			for (Path path: set) {
				System.err.println("Identified the existence of directory " + path);
			}
			System.err.println("Total: " + set.size() + " directories");
			System.err.println("Checking HDFS states");
			return;
		}

		File swapDir = null;
		try {
			if (localTmpFile.exists()) {
				FileUtils.delete(localTmpFile, FileUtils.RECURSIVE);
			}
			localTmpFile.mkdirs();
			FileSystem localFS = FileSystem.getLocal(fs.getConf());
			Path tmpTo = new Path(localTmpFile.getAbsolutePath());
			FileUtil.copy(fs, (Path [])set.toArray(), localFS, tmpTo, false, true, fs.getConf());

			//Set the last modification time to the original file to ensure the consistency.
			for (Path path: set) {
				File f = new File(localTmpFile.getAbsolutePath(), path.getName());
				f.setLastModified(fs.getFileStatus(path).getModificationTime());
			}

			hookCopyFrmHDFSToLocalFS(); //do nothing by default. But can be overrode

			if (localOutputFile.exists()) {
				swapDir = new File(localTmpFile.getParentFile(), LDAFiles.swapFileName);
				if (swapDir.exists()) {
					swapDir.delete();
				}
				swapDir.getParentFile().mkdirs();
				localOutputFile.renameTo(swapDir);
				System.err.println(localOutputFile.getAbsolutePath() + " exists. The directory is temporarily moved to " + swapDir.toString());
			}
			localOutputFile.getParentFile().mkdirs();
			localTmpFile.renameTo(localOutputFile);
			localOutputFile.setLastModified(fs.getFileStatus(outputPath).getModificationTime());
		} catch (Exception e) {
			System.err.println("Taking the snapshot of " + getJobName() + " aborts: " + e.toString());
			for (Path path: set) {
				System.err.println("Try copying " + path.toUri() + " to " + localOutputFile.toURI() +" by manual");
			}
		}
	}

	/**
	 * Check if all the directories exists in a local file system
	 * 
	 * @param checkedFiles keys are File to be checked and values are description of keys
	 * @return true If all the key files exists, otherwise false
	 */
	protected boolean filesExist(Map<File, String> checkedFiles) {
		Map<File, String> map = new HashMap<File, String>(checkedFiles);
		map.put(localArgFile, "Argument file for " + getJobName());
		map.put(localOutputFile, "Output directory" + getJobName());
		try {
			List<File> nonExists = new ArrayList<File>();
			for (File file: map.keySet()) {
				if (!file.exists()) {
					nonExists.add(file);
				}
			}
			if (!nonExists.isEmpty()) {
				System.err.println("Some of " + getJobName() + " outputs on a local disk are not found.");
				for (Map.Entry<File, String> e: map.entrySet()) {
					System.err.println(e.getValue() + " " + e.getKey().getAbsolutePath() + ": " + (nonExists.contains(e.getKey())? "found" : "not found"));
				}
				return false;
			}
			return true;
		} catch (Exception e) {
			System.err.println("Restoring the snapshot of cvb aborts: " + e.toString());
			System.err.println("Reading directories in a local disk failed.");
			System.err.println("Check permissions");
			return false;
		}
	}

	/**
	 * Copy the files from a local disk to HDFS.
	 * Use several swapping to ensure the exception/error safety which guarantees the consistency of job directory layout.
	 * This method deletes and overwrites all the existing destination
	 * 
	 * @param files Copied files on a local disk
	 */
	protected void copyFromLocalFSToHDFS(Set<File> files) {
		Set<Path> set = new HashSet<Path>();
		try {
			set.add(new Path(localArgFile.getAbsolutePath()));
			for (File file: files) {
				if (file.exists()) {
					set.add(new Path(file.getAbsolutePath()));
				}
			}
		} catch (Exception e) {
			System.err.println("Restoring the snapshot of " + getJobName() + " aborts: " + e.toString());
			System.err.println("Reading directories in a local disk failed: " + localOutputFile);
			System.err.println("Check permissions");
			return;
		}

		Path swapPath = null;
		try {
			if (fs.exists(tmpPath)) {
				fs.delete(tmpPath, true);
			}
			fs.mkdirs(tmpPath);
			FileSystem localFS = FileSystem.getLocal(fs.getConf());
			FileUtil.copy(localFS, (Path [])set.toArray(), fs, tmpPath, false, true, fs.getConf());

			//Set the last modification time to the original file to ensure the consistency.
			for (Path path: set) {
				Path p = new Path(tmpPath, path.getName());
				fs.setTimes(p, (new File(path.toString())).lastModified(), -1);
			}

			hookCopyFromLocalFSToHDFS(); //do nothing by default. But can be overrode

			if (fs.exists(outputPath)) {
				swapPath = new Path(tmpPath.getParent(), LDAFiles.swapFileName);
				if (fs.exists(swapPath)) {
					fs.delete(swapPath, true);
				}
				fs.mkdirs(swapPath.getParent());
				fs.rename(outputPath, swapPath);
				System.err.println(outputPath + " exists. The directory is temporarily moved to " + swapPath.toString());
			}
			fs.mkdirs(outputPath.getParent());
			fs.rename(tmpPath, outputPath);
			fs.setTimes(outputPath, localOutputFile.lastModified(), -1);
		} catch (Exception e) {
			System.err.println("Restoring the snapshot of " + getJobName() + " aborts: " + e.toString());
			for (Path path: set) {
				System.err.println("Try copying " + path.toUri() + " in a local disk to " + outputPath.toUri() +" in HDFS by manual");
			}
		}
	}

	/**
	 * Called by CopyFrom*To*() before the function commits the change to a file system.
	 * Users can override to do whatever users want at the timing.
	 * In particular, this function is required to hold the exception safety.
	 * Note: must handle the last modification time to ensure consistency if extra files are copied.
	 * 
	 * @throws Exception Users can throw any exceptions to stop CopyFrom*To*().
	 *                   The stop by this function does not break the exception safety.
	 */
	protected void hookCopyFrmHDFSToLocalFS() throws Exception {
	}

	protected void hookCopyFromLocalFSToHDFS() throws Exception {
	}

}
