package de.foam.data.import

import mu.KotlinLogging
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.*
import org.apache.hadoop.hbase.io.compress.Compression
import java.nio.file.Path
import java.net.URL
import java.nio.charset.Charset
import java.nio.file.Files
import java.util.concurrent.atomic.AtomicInteger

/**
 * @author jobusam
 * Upload data into HBASE. Use HBASE Java Client API to upload the data
 *
 * Advantages of this solution:
 * - File metadata is stored HBASE Table on Data Nodes / Region Servers
 * - Small files are directly persisted in HBASE (no HDFS metadata overhead)
 * - Better handling of specific files! Every file will be handled separately
 *   - content of symbolic links and character devices WON'T be resolved
 *      - upload metadata only
 * - Default environment urls for HDFS and HBASE are available and can be configured
 *
 *
 * Disadvantages:
 * - Files aren't directly accessible in HDFS
 * - For displaying directory hierarchy and files a custom viewer must be implemented.
 * - The original filesystem structure is not directly persisted in HDFS
 *
 * TODO:
 * - Add "/" to every relative file path. At the moment the imported root directory relative file path is empty!
 * - symbolic links are recognized but their reference target should also be stored in HBASE
 * - Compile Hadoop native lib (libhadoop.so) for x64 Fedora to improve performance!
 * - Overall performance must be improved. The data import needs to much time!
 *   - But at the moment the file uploads aren't executed in parallel!
 *
 * Result:
 * - This implementation works good but must be improved to work faster. At the moment
 *   the upload needs a lot of time. Therefore the upload time must be reduced and it
 *   would be really nice to start with data analysis already during the file import!
 *
 * Alternative way:
 * There is also another column-bases database called Apache Accumulo available
 * in HADOOP ecosystem. The APIs are slightly different but the performance should
 * be the same (acoording to several blog posts).
 *
 */
const val TABLE_NAME_FORENSIC_DATA = "forensicData"
const val COLUMN_FAMILY_NAME_CONTENT = "content"
const val COLUMN_FAMILY_NAME_METADATA = "metadata"

class HbaseImport(private val inputDirectory: Path, hbaseSiteXML: Path?, private val hdfsImport: HDFSImport) {

    private val logger = KotlinLogging.logger {}
    private val utf8 = Charset.forName("utf-8")
    private var connection: Connection? = null

    private val rowCount = AtomicInteger()
    // for statistics only
    private val fileContentsInHbase = AtomicInteger()
    private val fileContentsInHdfs = AtomicInteger()


    /**
     * Initial connection to HBASE and share it for all data uploads.
     * TODO: Use one common configuration object for HDFS and HBASE. At the moment
     * this implementation also works with Kerberos. But this is only a side effect,
     * because Kerberos is already configured in HDFSImport!
     */
    init {
        logger.info { "Try to connect to HBASE..." }
        val url: URL? = hbaseSiteXML?.toUri()?.toURL() ?: HbaseImport::class.java.getResource("/hbase-site-client.xml")
        url?.let {
            logger.info { "Use configuration file $url" }
            val config = HBaseConfiguration.create()
            config.addResource(org.apache.hadoop.fs.Path(url.path))
            HBaseAdmin.checkHBaseAvailable(config)
            connection = ConnectionFactory.createConnection(config)
        }
    }

    /**
     * Delete old table with old content (if any exists)
     * and create a new table without content
     */
    private fun createOrOverwrite(admin: Admin, table: HTableDescriptor) {
        if (admin.tableExists(table.tableName)) {
            if (admin.isTableEnabled(table.tableName)) {
                admin.disableTable(table.tableName)
            }
            admin.deleteTable(table.tableName)
        }
        admin.createTable(table)
    }

    /**
     * Create Tables for persisting files and file metadata in HBASE and HDFS
     */
    fun createTables() {
        connection?.let { connection ->
            connection.admin.use { admin ->

                val table = HTableDescriptor(TableName.valueOf(TABLE_NAME_FORENSIC_DATA))
                table.addFamily(HColumnDescriptor(COLUMN_FAMILY_NAME_METADATA).setCompressionType(Compression.Algorithm.NONE))
                table.addFamily(HColumnDescriptor(COLUMN_FAMILY_NAME_CONTENT).setCompressionType(Compression.Algorithm.NONE))

                logger.debug { "Creating table $table in HBASE" }
                createOrOverwrite(admin, table)
                logger.trace { "Finished creation of table $table done" }
            }
        }
    }

    /**
     * Upload a single file content and the given metadata into HBASE. If the file is
     * very small (smaller than 10 MB ,see isSmallFile() method) than upload the file content directly
     * into HBASE table. Otherwise upload the large file content into HDFS.
     */
    fun uploadFile(fileMetadata: FileMetadata) {
        connection?.let { connection ->
            logger.trace { "Upload Metadata of file ${inputDirectory.resolve(fileMetadata.relativeFilePath)}" }
            val table = connection.getTable(TableName.valueOf(TABLE_NAME_FORENSIC_DATA))
            val row = "row" + rowCount.getAndIncrement()
            table.put(createPuts(fileMetadata, row))

            if (FileType.DATA_FILE == fileMetadata.fileType && !isSmallFile(fileMetadata)) {
                // Use row index of hbase entry as file name for raw file content
                hdfsImport.uploadFileIntoHDFS(fileMetadata.relativeFilePath, row)
            }
        }
    }

    /**
     * create Put objects to import every metadata into a single column
     */
    private fun createPuts(fileMetadata: FileMetadata, row: String): List<Put> {


        val map = fileMetadata.toMap().map {
            Put(row.toByteArray(utf8)).addColumn(COLUMN_FAMILY_NAME_METADATA.toByteArray(utf8),
                    it.key.toByteArray(utf8),
                    it.value.toByteArray(utf8))
        }

        if (FileType.DATA_FILE == fileMetadata.fileType) {
            return if (isSmallFile(fileMetadata)) {
                fileContentsInHbase.incrementAndGet()
                val absolutePath = inputDirectory.resolve(fileMetadata.relativeFilePath)
                map.plus(Put(row.toByteArray(utf8)).addColumn(COLUMN_FAMILY_NAME_CONTENT.toByteArray(utf8),
                        "fileContent".toByteArray(utf8),
                        Files.readAllBytes(absolutePath)))
            } else {
                //It's a large file. Therefore only save a file path in the database column "hdfsFilePath"
                // Additionally the row index will be used as file name for the raw content in hdfs!
                fileContentsInHdfs.incrementAndGet()
                map.plus(Put(row.toByteArray(utf8)).addColumn(COLUMN_FAMILY_NAME_CONTENT.toByteArray(utf8),
                        "hdfsFilePath".toByteArray(utf8),
                        hdfsImport.getHDFSBaseDirectory().resolve(row).toString().toByteArray(utf8)))
            }
        }
        return map
    }

    private fun isSmallFile(fileMetadata: FileMetadata): Boolean {
        // The default maximum keyvalue size is 10 MB (10485760 Bytes)
        // 10485660 (100 Bytes reserved for key itself ;) )
        // Keep in mind. The default size in Hortonworks Data platform is 1 MB and should be increased at least to 10 MB!
        return fileMetadata.fileSize != null && 10485660 >= fileMetadata.fileSize
    }

    /**
     * Close any open connection to HBASE. Call this method to release all resources of this instance!
     */
    fun closeConnection() {
        logger.info {
            "Close Connections after uploading $rowCount files! " +
                    "(Small files in HBASE = $fileContentsInHbase; " +
                    "Large files  in HDFS = $fileContentsInHdfs)"
        }
        connection?.close()
        connection = null
    }

}
