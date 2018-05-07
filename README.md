# foam-data-import
This project contains a forensic application for importing a directory with arbitrary files
into Apache Hadoop HDFS and HBASE.

## Current functionality
At the moment it's possible to upload an directory with arbitrary files into HDFS and HBASE.   
+ Read file metadata and upload into HBASE DB:
  + Upload path, file size, file type, owner, group, permissions and MAC timestamps.
+ Upload raw file content:
  + Small files will be uploaded directly into HBASE db (for a better performance).
  + Large files will be uploaded directly into HDFS (predefined directory).

See also implementation for further details.

## Build Forensic Import App
Checkout the Gradle Project written in Kotlin.

Install Java JDK 1.8.0 (optional)
```
# on Fedora 27
sudo dnf install java-1.8.0-openjdk 
```

Build it with  the provided gradle wrapper.
```
# Build JAR file
./gradlew build

# Build a fat JAR file containing also the required dependencies
./gradlew buildFatJar
```
The fat JAR is located under build/libs/data.import-1.0-SNAPSHOT-capsule.jar

## Run the Forensic Data Import App

For execution a working instance of Hadoop HDFS and HBASE is required.
 
For setup a local standalone instance of HDFS an HBASE 
please refer to project [foam-storage-hadoop](https://github.com/jobusam/foam-storage-hadoop).

```
# Execute the jar file
java -jar data.import-1.0-SNAPSHOT-capsule.jar path_to_local_src_dir path_to_hdfs_target_dir \
[path_to_hbase-site.xml] [path_to_hadoop_core-site.xml]

# 1. param: contains the path to local src directory that shall be imported into HBASE/HDFS.
# 2. param: contains the remote path in the hdfs filesystem where to upload large file contents
#           (small files will be uploaded directly into HBASE).
# 3. param: optional HBASE configuraion. 
#           If not given use localhost:2181 for connecting to zookeeper.
# 4. param: optional HADOOP configuration. 
#           If not given use default fs uri hdfs://localhost:9000.
```

## Other
Tested on Fedora 27. 

Feel free to give me feedback if something doesn't work with your setup.
