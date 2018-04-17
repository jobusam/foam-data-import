package de.foam.data.import

import com.google.gson.Gson
import java.nio.file.Path
import java.util.*

/**
 * @author jobusam
 * Upload data into local HDFS. Use shell "hdfs" command for upload files and metadata.
 * The file metadata will be serialized into JSON and will be uploaded as
 * extended metadata attribute of the given file.
 *
 * Advantages of this solution:
 * - Files are directly accessible in HDFS
 * - Same file structure like in original data directory
 *
 * Disadvantages:
 * - File metadata is stored on NameNode! This absolutely unacceptable for large data processing!
 * - Inefficient processing and persistence of small files because every file will be saved as separate
 *   file on HDFS (see HADOOP HDFS Handling of small files).
 * - Environment must be set up before!
 *  -- "hdfs" command must be available
 *  -- Default hdfs must be configured correctly
 * - Raw data upload can be done recursively in one command.
 *   But metadata upload must be done per file. This is a lot of overhead because for every
 *   command execution an own sub process will be created. Only for a bunch of files (up to 20 files)
 *   the upload takes a while.
 * - Symbolic links on local file system will be resolved with "hdfs dfs -put" command.
 *   This leads to much more overhead on HDFS!
 *
 */

const val HADOOP_HOME_BIN = "/home/johannes/Studium/Masterthesis/work/localinstance/hadoop-3.0.0/bin"

/**
 * Upload local input directory int Hadoop HDFS.
 * The default HDFS configuration will be used to access the HDFS (see $HADOOP_HOME/etc/hadoop)
 */
fun uploadContentToHDFS(inputDirectory: Path, hdfsDirectory: Path): Boolean {
    val command = Arrays.asList("$HADOOP_HOME_BIN/hdfs", "dfs", "-put", inputDirectory.toAbsolutePath().toString(), hdfsDirectory.toString())
    val processBuilder = ProcessBuilder(command)
    println("Execute command ${processBuilder.command()}")
    val result = processBuilder.start().waitFor()

    if (0 == result) {
        println("Command successfully executed!")
        return true
    }
    println("Command execution failed!")
    return false
}

// fun deserializeMetadata(serializedMetadata: String):FileMetadata = Gson().fromJson(serializedMetadata,FileMetadata::class.java)

// use GsonBuilder().setPrettyPrinting().create() for pretty serialization
fun serializeMetadata(fileMetadata: FileMetadata):String = Gson().toJson(fileMetadata)

fun uploadFileMetadata(fileMetadata: FileMetadata,hdfsDirectory: Path){
    val serializedMetadata = serializeMetadata(fileMetadata)
    println("Serialized Metadata = $serializedMetadata")

    val command = Arrays.asList("$HADOOP_HOME_BIN/hdfs", "dfs", "-setfattr",
            "-n","user.originalMetadata",
            "-v","\"$serializedMetadata\"",
            "${hdfsDirectory.resolve(fileMetadata.relativeFilePath)}")
    val processBuilder = ProcessBuilder(command)
    println("Execute command ${processBuilder.command()}")

    //If the app doesn't wait until the subprocess is finished, the performance would be quite better.
    //But otherwise on local standalone hadoop instance the memory consumption increases a lot for a short time interval.
    processBuilder.start().waitFor()
}
