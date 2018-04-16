package de.foam.data.import

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
 *   command execution an own sub process will be created
 * - Symbolic links on local file system will be resolved with "hdfs dfs -put" command.
 *   This leads to much more overhead on HDFS!
 *
 */

const val HADOOP_HOME_BIN = "/home/johannes/Studium/Masterthesis/work/localinstance/hadoop-3.0.0/bin"

/**
 * Upload local input directory int Hadoop HDFS.
 * The default HDFS configuration will be used to access the HDFS (see $HADOO_HOME/etc/hadoop)
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