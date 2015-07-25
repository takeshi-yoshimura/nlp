A simple tool for NLP
======================

The primary purpose of this tool is to get rid of stressful data managements with Mahout and Hadoop.
Thus, it basically wraps Mahout and Hadoop with simple command line interfaces, but also provides some utilities.

## Requirement

maven, jdk1.7 (builds with other jdk may fail), hadoop-2.6.0-cdh5.4.4, mahout-0.9-cdh5.4.4

## Build

```bash
$ mvn package
```

## Run

```bash
$ vi conf.json
$ vi run
```

Configure your environments

```bash
$ su {hadoop user}
$ ./run
```

Available commands are displayed if no arguments

## Develop with Eclipse

```bash
$ mvn eclipse:eclipse
```

Note: you may encounter jdk.tools warnings on pom.xml if you convert the project to a Maven project.

## License

MIT

## TODO

* DeleteJob
	* Deletes job results on HDFS
	* Hides HDFS from users more

* Result decorator for Hive queries
	* Allows users to promptly analyze data by Mahout
	* Needs VectorWritable parser for Hive

* Better logging

* Stopping Maven directory layout
	* Moves target/ and eclipse settings out of tree for Git-friendly
	* CMake?

* Spark movement
	* Potentially speeds up everything
	* But needs to consider high memory pressures
	* Parameter Server?

* Job history and statistics collections
	* e.g., Hadoop job configuration, task counters (.xml and .jhist files)
	* May be useful for future uses

* Add other data analytics
	* Machine learning, graph, etc.

## Author
Takeshi Yoshimura (https://github.com/takeshi-yoshimura)
