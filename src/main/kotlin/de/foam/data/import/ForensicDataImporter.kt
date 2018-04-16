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

    val filePath = Paths.get("/home/johannes/Studium/Masterthesis/work/localinstance/apps/data-minimal")
    println("Input data directory is $filePath")

    val rootDirectory = filePath.toFile()
    rootDirectory.walk(FileWalkDirection.TOP_DOWN)
            .map { getFileMetadata(it.toPath()) }
            .forEach { it?.let { println(it) } }

}

