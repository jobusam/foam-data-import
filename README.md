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
```
The application is packaged as zip/tar archive located in folder build/distributions/data.import-[...]  

## Run the Forensic Data Import App

For execution a working instance of Hadoop HDFS and HBASE is required.
 
For setup a local standalone instance of HDFS an HBASE 
please refer to project [foam-storage-hadoop](https://github.com/jobusam/foam-storage-hadoop).

```
# Unzip the application package
unzip data.import-1.0-SNAPSHOT.zip

# Run the application via start script
bin/data-import local_source_directory

# "local_source_directory" contains the path to local src directory that shall be imported into HBASE/HDFS.


# Alternatively display the help page with all options and further informations.
bin/data-import --help 

```

## Other
Tested on Fedora 27 and CentOS 7 with Hortonworks HDP 2.6.5.

Feel free to give me feedback if something doesn't work with your setup.
