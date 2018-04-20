package de.foam.data.import

import com.google.gson.Gson
import mu.KotlinLogging
import java.nio.file.Path
import java.util.*
import java.util.concurrent.Callable
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
 *   CAUTION: The "hdfs -put" command hangs up in endless loop in case a symbolic link file
 *   refers to a non-existent file!
 *   This is a huge drawback because operating systems like Linux images does contain a lot of symbolic links
 *   that refer to files which are only existent during runtime (like special devices, etc.).
 *
 *
 * Result:
 * This variant needs a loot of time for upload and doesn't work correctly with symbolic links!
 *
 */
class HdfsImportCli(
        private val hadoopHome: Path?,
        private val hdfsTargetDirectory: Path,
        private val inputDirectory: Path) {

    private val logger = KotlinLogging.logger {}
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
        val hdfsCommand = hadoopHome?.resolve("bin/hdfs")?.toString() ?: "hdfs"
        val command = Arrays.asList(hdfsCommand, "dfs", "-put", inputDirectory.toAbsolutePath().toString(), hdfsTargetDirectory.toString())
        val processBuilder = ProcessBuilder(command)
        logger.debug { "Execute command ${processBuilder.command()}" }
        val result = processBuilder.start().waitFor()

        if (0 == result) {
            logger.debug { "Command $command successfully executed!" }
            return true
        }
        logger.error { "Command execution failed <$command>"}
        return false
    }

    // use GsonBuilder().setPrettyPrinting().create() for pretty serialization
    private fun serializeMetadata(fileMetadata: FileMetadata): String = Gson().toJson(fileMetadata)


    fun uploadFileMetadata(fileMetadata: FileMetadata) {
        val serializedMetadata = serializeMetadata(fileMetadata)
        logger.trace { "Serialized Metadata = $serializedMetadata"}

        val hdfsCommand = hadoopHome?.resolve("bin/hdfs")?.toString() ?: "hdfs"
        val command = Arrays.asList(hdfsCommand, "dfs", "-setfattr",
                "-n", "user.originalMetadata",
                "-v", "\"$serializedMetadata\"",
                "${hdfsTargetDirectory.resolve(fileMetadata.relativeFilePath)}")
        val processBuilder = ProcessBuilder(command)
        logger.debug { "Execute command ${processBuilder.command()}" }

        // Maybe the best result is to work with an Thread Pool. Because synchronous execution needs to much time
        // but unlimited asynchronous execution leads to exponential resource consumption for a short period of time!
        service?.submit( Callable<Int> {processBuilder.start().waitFor()} )?.let { futures.add(it) }
               ?: logger.error { "ERROR: Can't upload Metadata. Executor Service is not available!" }
    }

}