package de.foam.data.import

import com.google.gson.Gson
import java.nio.file.Path
import java.util.*
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.Future


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
class HdfsImportCli(
        val hadoopHomeBin: String = "/home/johannes/Studium/Masterthesis/work/localinstance/hadoop-3.0.0/bin",
        val hdfsTargetDirectory: Path,
        val inputDirectory: Path) {

    private val service: ExecutorService? = Executors.newFixedThreadPool(10)
    private val futures = mutableListOf<Future<Int>>()


    fun waitForExecution() {
        //Wait for every execution result
        futures.forEach { it.get() }
        service?.shutdown()
    }

    /**
     * Upload local input directory int Hadoop HDFS.
     * The default HDFS configuration will be used to access the HDFS (see $HADOOP_HOME/etc/hadoop)
     */
    fun uploadContentToHDFS(): Boolean {
        val command = Arrays.asList("$hadoopHomeBin/hdfs", "dfs", "-put", inputDirectory.toAbsolutePath().toString(), hdfsTargetDirectory.toString())
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

    // use GsonBuilder().setPrettyPrinting().create() for pretty serialization
    private fun serializeMetadata(fileMetadata: FileMetadata): String = Gson().toJson(fileMetadata)


    fun uploadFileMetadata(fileMetadata: FileMetadata) {
        val serializedMetadata = serializeMetadata(fileMetadata)
        println("Serialized Metadata = $serializedMetadata")


        val command = Arrays.asList("$hadoopHomeBin/hdfs", "dfs", "-setfattr",
                "-n", "user.originalMetadata",
                "-v", "\"$serializedMetadata\"",
                "${hdfsTargetDirectory.resolve(fileMetadata.relativeFilePath)}")
        val processBuilder = ProcessBuilder(command)
        println("Execute command ${processBuilder.command()}")

        // Maybe the best result is to work with an Thread Pool. Because synchronous execution needs to much time
        // but unlimited asynchronous execution leads to exponential resource consumption for a short period of time!
        service?.submit { processBuilder.start().waitFor() }?.let { futures.add(it as Future<Int>) }
               ?: println("ERROR: Can't upload Metadata. Executor Service is not available!")
    }

}