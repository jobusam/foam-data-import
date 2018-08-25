package de.foam.dataimport

import java.nio.file.Files
import java.nio.file.LinkOption
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
data class FileMetadata(val relativeFilePath: String, val fileType: FileType, val fileSize: Long?,
                        val owner: String, val group: String, val permissions: String,
                        val timestamps: FileTimestamps
){

    /**
     * Copy all properties into an hash map <propertyName,propertyValue>.
     * This could be also done with reflection and foo::class.memberProperties method.
     * But the reflection is more complex for embedded types. Also the api is more constant because,
     * this property names will be later used in HBASE DB for metadata access...
     */
    fun toMap(): Map<String,String>{
        // TODO: review! Maybe it's better to don't return optional values!
        val values = mutableMapOf<String,String>()
        values["filePath"] = this.relativeFilePath
        values["fileType"] = this.fileType.toString()
        values["fileSize"] = this.fileSize?.toString() ?: ""
        values["owner"] = this.owner
        values["group"] = this.group
        values["permissions"] = this.permissions
        values["lastModified"] = this.timestamps.lastModified
        values["lastChanged"] = this.timestamps.lastChanged ?: ""
        values["lastAccessed"] = this.timestamps.lastModified
        values["created"] = this.timestamps.lastModified
        return values
    }
}

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
 * Retrieve file metadata from given filePath like owner, group, permissions and timestamps.
 * Keep in mind file timestamps should be always defined in UTC Timezone!
 */
fun getFileMetadata(filePath: Path, inputDirectory: Path): FileMetadata? {
    // Don't follow symbolic links! (e.g. symbolic link can link to an file oustide the mounted/given image and it's not guaranteed that this linked file exists!
    // see file /etc/cups/ssl/server.crt in ubuntu image v.1.0 -> the file itself is an symbolic link and the target file doesn't exist anymore!
    val posixAttributes = Files.getFileAttributeView(filePath, PosixFileAttributeView::class.java,LinkOption.NOFOLLOW_LINKS)?.readAttributes()
    val fileType = when {
        Files.isSymbolicLink(filePath) -> FileType.SYMBOLIC_LINK
        Files.isRegularFile(filePath) -> FileType.DATA_FILE
        Files.isDirectory(filePath) -> FileType.DIRECTORY
        else -> FileType.OTHER
    }


    posixAttributes?.let {

        val timestamps = FileTimestamps(posixAttributes.lastModifiedTime().toString(),
                lastAccessed = posixAttributes.lastAccessTime().toString(),
                created = posixAttributes.creationTime().toString())
        return FileMetadata(
                inputDirectory.relativize(filePath).toString(),
                fileType,
                posixAttributes.size(),
                posixAttributes.owner().toString(),
                posixAttributes.group().toString(),
                posixAttributes.permissions().toString(),
                timestamps)
    }
    return null
}