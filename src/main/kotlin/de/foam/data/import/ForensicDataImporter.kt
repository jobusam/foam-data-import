package de.foam.data.import

import java.nio.file.Paths

/**
 * @author jobusam
 *
 * Upload all files of the given directory into Apache Hadoop.
 * Also preserve the original file metadata from local fs.
 * Different approaches to preserve the file metadata will be implemented
 * and reviewed in this simple application.
 */
fun main(args: Array<String>) {
    println("Starting Forensic Data Import...")

    val inputDirectory = Paths.get("/home/johannes/Studium/Masterthesis/work/localinstance/apps/data-minimal")
    val hdfsDirectoryPath = Paths.get("/data-minimal")
    println("Data from input data directory <$inputDirectory> will be uploaded to hdfs target directory <$hdfsDirectoryPath>")

    val hdfsImportCli = HdfsImportCli(hdfsTargetDirectory = hdfsDirectoryPath, inputDirectory = inputDirectory)

    val uploaded = hdfsImportCli.uploadContentToHDFS()
    if (!uploaded) {
        println("File upload of directory $inputDirectory failed. Stop Forensic Data Import!")
        return
    }

    val rootDirectory = inputDirectory.toFile()
    rootDirectory.walk(FileWalkDirection.TOP_DOWN)
            .map { getFileMetadata(it.toPath(),inputDirectory) }
            .onEach { it?.let { println(it) } }
            .forEach { it?.let {hdfsImportCli.uploadFileMetadata(it)} }

    hdfsImportCli.waitForExecution()
    println("File Metadata uploaded. Forensic Data Import finished.")

}


