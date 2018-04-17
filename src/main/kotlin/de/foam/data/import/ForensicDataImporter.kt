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
    println("Input data directory is $inputDirectory")

    val uploaded = uploadContentToHDFS(inputDirectory, hdfsDirectoryPath)
    if (!uploaded) {
        println("File upload of directory $inputDirectory failed. Stop Forensic Data Import!")
        return
    }

    val rootDirectory = inputDirectory.toFile()
    rootDirectory.walk(FileWalkDirection.TOP_DOWN)
            .map { getFileMetadata(it.toPath(),inputDirectory) }
            .also { it.let { println(it) } }
            .onEach { it?.let { println(it) } }
            .forEach { it?.let {uploadFileMetadata(it,hdfsDirectoryPath)} }
}


