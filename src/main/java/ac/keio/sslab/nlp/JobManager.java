package ac.keio.sslab.nlp;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.io.IOUtils;
import org.json.JSONObject;

public class JobManager {

	protected final NLPConf conf = new NLPConf();
	protected File argFile;
	protected File lockFile;

	public JobManager(String jobName) {
		argFile = new File(conf.localArgFile, jobName);
		lockFile = new File(conf.localLockFile, jobName);
		argFile.getParentFile().mkdirs();
		lockFile.mkdirs();
	}

	public boolean tryLock(String jobID) {
		try {
			File lock = new File(lockFile, jobID);
			if (lock.exists()) {
				return false;
			} else {
				lock.createNewFile();
				lock.deleteOnExit(); //Does this work?
				return true;
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("Trylock failed: " + e.toString());
			return false;
		}
	}

	public void unLock(String jobID) {
		try {
			File lock = new File(lockFile, jobID);
			if (lock.exists()) {
				//lock.delete();
			} else {
				System.err.println("WARNING: Inconsistent unlock");
			}
		} catch (Exception e) {
			System.err.println("Unlock failed: " + e.toString());
		}
	}

	public boolean hasJobIDArgs(String jobID) {
		if (!argFile.exists()) {
			return false;
		}
		FileInputStream inputStream = null;
	    try {
			inputStream = new FileInputStream(argFile);
			JSONObject reader = new JSONObject(IOUtils.toString(inputStream));
			boolean hasJobID = reader.has(jobID);
	        inputStream.close();
	        return hasJobID;
	    } catch (Exception e) {
	    	System.err.println("Reading json " + argFile.getAbsolutePath() + " failed: " + e.toString());
	    	return false;
	    }
	}

	public Map<String, String> getJobIDArgs(String jobID) {
	    try {
			Map<String, String> map = new HashMap<String, String>();
	    	FileInputStream inputStream = new FileInputStream(argFile);
			JSONObject jobJson = new JSONObject(IOUtils.toString(inputStream));
			JSONObject jobIDJson = jobJson.getJSONObject(jobID);
			for (String key: jobIDJson.keySet()) {
				map.put(key, jobIDJson.getString(key));
			}
	        inputStream.close();
	        return map;
	    } catch (Exception e) {
	    	System.err.println("Reading json " + argFile.getAbsolutePath() + " failed: " + e.toString());
	    	return null;
	    }
	}

	public void saveJobIDArgs(String jobID, Map<String, String> args) {
	    try {
			FileInputStream inputStream;
			JSONObject jobJson;
			if (!argFile.exists()) {
				jobJson = new JSONObject();
				argFile.createNewFile();
				inputStream = new FileInputStream(argFile);
			} else {
				inputStream = new FileInputStream(argFile);
				jobJson = new JSONObject(IOUtils.toString(inputStream));
			}
			if (jobJson.has(jobID)) {
				jobJson.remove(jobID);
			}
			Map<String, String> newArgs = new HashMap<String, String>();
			for (Entry<String, String> e: args.entrySet()) {
				if (e.getKey().equals("ow") || e.getKey().equals("f")) {
					continue;
				}
				newArgs.put(e.getKey(), e.getValue());
			}
			jobJson.put(jobID, args);
	        inputStream.close();
	        FileOutputStream outputStream = new FileOutputStream(argFile);
	        outputStream.write(jobJson.toString(4).getBytes());
	        outputStream.close();
	    } catch (Exception e) {
	    	System.err.println("Reading or writing json " + argFile.getAbsolutePath() + " failed: " + e.toString());
	    }
	}
}
