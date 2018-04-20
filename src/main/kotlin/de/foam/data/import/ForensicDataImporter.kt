package de.foam.data.import

import mu.KotlinLogging
import java.nio.file.Path
import java.nio.file.Paths

/**
 * @author jobusam
 *
 * Upload all files of the given directory into Apache Hadoop.
 * Also preserve the original file metadata from local fs.
 * Different approaches to preserve the file metadata will be implemented
 * and reviewed in this simple application.
 */

/**
 * Use kotlin-logging (Micro-Utils).
 * Define log level with -Dorg.slf4j.simpleLogger.defaultLogLevel=DEBUG in vm options
 */
private val logger = KotlinLogging.logger {}


fun main(args: Array<String>) {
    logger.info { "Starting Forensic Data Import..." }
    dataImportVariantWithCLICommand(args)
    logger.info { "File Metadata uploaded. Forensic Data Import finished." }

}

/**
 * Upload data into local HDFS. Use shell "hdfs" command for upload files and metadata.
 * See class HdfsImportCli for further details on this implementation
 */
fun dataImportVariantWithCLICommand(args: Array<String>){

    val inputDirectory: Path
    val hdfsDirectoryPath: Path
    val hadoopHome: Path?
    when (args.size) {
        2 -> {
            inputDirectory = Paths.get(args[0])
            hdfsDirectoryPath = Paths.get(args[1])
            hadoopHome = null
        }
        3 -> {
            inputDirectory = Paths.get(args[0])
            hdfsDirectoryPath = Paths.get(args[1])
            hadoopHome = Paths.get(args[2])
        }
        else -> {
            logger.error {
                "Wrong arguments! Follow syntax is provided:\n" +
                        "LOCAL_SOURCE_DIR HDFS_TARGET_DIR [HDFS_HOME]\n" +
                        "Use absolute paths for all arguments. "
            }
            return
        }
    }


    logger.info {
        "Data from input data directory <$inputDirectory>" +
                " will be uploaded to hdfs target directory <$hdfsDirectoryPath>"
    }

    val hdfsImportCli = HdfsImportCli(hadoopHome, hdfsDirectoryPath, inputDirectory)

    val uploaded = hdfsImportCli.uploadContentToHDFS()
    if (!uploaded) {
        logger.error { "File upload of directory $inputDirectory failed. Stop Forensic Data Import!" }
        return
    }

    val rootDirectory = inputDirectory.toFile()
    rootDirectory.walk(FileWalkDirection.TOP_DOWN)
            .map { getFileMetadata(it.toPath(), inputDirectory) }
            .onEach { it?.let { println(it) } }
            .forEach { it?.let { hdfsImportCli.uploadFileMetadata(it) } }

    hdfsImportCli.waitForExecution()
}


