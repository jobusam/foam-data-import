package de.foam.data.import

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.attribute.PosixFileAttributeView

/**
 * @author jobusam
 *
 * Retrieve file metadata
 */


/**
 * Contains all metadata of a file
 */
data class FileMetadata(val filePath: Path, val fileType: FileType, val fileSize: Long?,
                        val owner: String, val group: String, val permissions: String,
                        val timestamps: FileTimestamps
)

/**
 * Encapsulates file timestamps. Normally lastModified is always given.
 * LastAccessed must be supported by the given OS to get real values.
 * The value lastChanged defines, when the metadata of the file was changed
 * (and not the content). This is supported by ext4 file system.
 * The created statement is also filesystem specific. NTFS supports that but ext4 does not.
 */
data class FileTimestamps(
        val lastModified: String, val lastChanged: String? = null,
        val lastAccessed: String, val created: String
)

enum class FileType { DATA_FILE, DIRECTORY, SYMBOLIC_LINK, OTHER }

/**
 * Retrieve file metadata from given filePath like owner, group, permissions and timestamps
 */
fun getFileMetadata(filePath: Path): FileMetadata? {
    val posixAttributes = Files.getFileAttributeView(filePath, PosixFileAttributeView::class.java)?.readAttributes()

    val fileType: FileType
    if (Files.isSymbolicLink(filePath)) {
        fileType = FileType.SYMBOLIC_LINK
    } else if (Files.isRegularFile(filePath)) {
        fileType = FileType.DATA_FILE
    } else if (Files.isDirectory(filePath)) {
        fileType = FileType.DIRECTORY
    } else {
        fileType = FileType.OTHER
    }


    posixAttributes?.let({

        val timestamps = FileTimestamps(posixAttributes.lastModifiedTime().toString(),
                lastAccessed = posixAttributes.lastAccessTime().toString(),
                created = posixAttributes.creationTime().toString())
        return FileMetadata(filePath, fileType, Files.size(filePath),
                posixAttributes.owner().toString(),
                posixAttributes.group().toString(),
                posixAttributes.permissions().toString(),
                timestamps)
    })
    return null
}
