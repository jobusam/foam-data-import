package de.foam.dataimport

import de.foam.dataimport.casemanagement.ForensicCase
import de.foam.dataimport.casemanagement.ForensicCaseManager
import mu.KotlinLogging
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.compress.Compression
import java.nio.charset.Charset
import java.nio.file.Files
import java.nio.file.Path
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
 * be the same (according to several blog posts).
 *
 */
const val TABLE_NAME_FORENSIC_DATA = "forensicData"
const val COLUMN_FAMILY_NAME_CONTENT = "content"
const val COLUMN_FAMILY_NAME_METADATA = "metadata"

class HbaseDataImport(private val inputDirectory: Path, private val hdfsDataImport: HDFSDataImport,
                      forensicCase: ForensicCase) {

    private val logger = KotlinLogging.logger {}
    private val utf8 = Charset.forName("utf-8")

    private val rowPrefix: String
    private val rowCount = AtomicInteger()

    // for statistics only
    private val fileContentsInHbase = AtomicInteger()
    private val fileContentsInHdfs = AtomicInteger()

    private val caseManager = ForensicCaseManager()


    /**
     * Initial connection to HBASE and share it for all data uploads.
     * TODO: Use one common configuration object for HDFS and HBASE. At the moment
     * this implementation also works with Kerberos. But this is only a side effect,
     * because Kerberos is already configured in HDFSDataImport!
     */
    init {


        logger.info { "Create Tables in HBASE" }
        createTables()

        val exhibitId = caseManager.createNewCase(forensicCase) ?: ""
        rowPrefix = exhibitId
        hdfsDataImport.createHDFSExhibitDiretory(exhibitId)
    }

    /**
     * Create Tables for forensic case management and file storage (incl. metadata) in HBASE.
     */
    private fun createTables() {
        caseManager.createForensicCaseManagementTables()
        caseManager.createTable(createForensicDataTableDescriptor())
    }

    private fun createForensicDataTableDescriptor(): HTableDescriptor {
        val table = HTableDescriptor(TableName.valueOf(TABLE_NAME_FORENSIC_DATA))
        table.addFamily(HColumnDescriptor(COLUMN_FAMILY_NAME_METADATA)
                // Set the replication scope to 1 is very important for replicate data with hbase-indexer to solr!
                .setCompressionType(Compression.Algorithm.NONE).setScope(1))
        table.addFamily(HColumnDescriptor(COLUMN_FAMILY_NAME_CONTENT).setCompressionType(Compression.Algorithm.NONE))
        return table
    }

    /**
     * Delete Tables for forensic case management and file storage (incl. metadata) in HBASE.
     */
    fun deleteTables() {
        caseManager.deleteForensicCaseManagementTables()
        caseManager.deleteTable(TABLE_NAME_FORENSIC_DATA)
    }

    /**
     * Upload a single file content and the given metadata into HBASE. If the file is
     * very small (smaller than 10 MB ,see isSmallFile() method) than upload the file content directly
     * into HBASE table. Otherwise upload the large file content into HDFS.
     */
    fun uploadFile(fileMetadata: FileMetadata) {
        HBaseConnection.connection?.let { connection ->
            logger.trace { "Upload Metadata of file ${inputDirectory.resolve(fileMetadata.relativeFilePath)}" }
            val table = connection.getTable(TableName.valueOf(TABLE_NAME_FORENSIC_DATA))
            val rowKey = "${rowPrefix}_${rowCount.getAndIncrement()}"
            table.put(createPuts(fileMetadata, rowKey))
            table.close()
            if (FileType.DATA_FILE == fileMetadata.fileType && !isSmallFile(fileMetadata)) {
                // Use row index of hbase entry as file name for raw file content
                hdfsDataImport.uploadFileIntoHDFS(fileMetadata.relativeFilePath, rowKey)
            }
        }
    }

    /**
     * create Put objects to import every metadata into a single column
     */
    private fun createPuts(fileMetadata: FileMetadata, rowKey: String): List<Put> {


        val map = fileMetadata.toMap().map {
            Put(rowKey.toByteArray(utf8)).addColumn(COLUMN_FAMILY_NAME_METADATA.toByteArray(utf8),
                    it.key.toByteArray(utf8),
                    it.value.toByteArray(utf8))
        }

        if (FileType.DATA_FILE == fileMetadata.fileType) {
            return if (isSmallFile(fileMetadata)) {
                fileContentsInHbase.incrementAndGet()
                val absolutePath = inputDirectory.resolve(fileMetadata.relativeFilePath)
                map.plus(Put(rowKey.toByteArray(utf8)).addColumn(COLUMN_FAMILY_NAME_CONTENT.toByteArray(utf8),
                        "fileContent".toByteArray(utf8),
                        Files.readAllBytes(absolutePath)))
            } else {
                //It's a large file. Therefore only save a file path in the database column "hdfsFilePath"
                // Additionally the row index will be used as file name for the raw content in hdfs!
                fileContentsInHdfs.incrementAndGet()
                map.plus(Put(rowKey.toByteArray(utf8)).addColumn(COLUMN_FAMILY_NAME_CONTENT.toByteArray(utf8),
                        "hdfsFilePath".toByteArray(utf8),
                        hdfsDataImport.getHDFSExhibitDirectory().resolve(rowKey).toString().toByteArray(utf8)))
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
    fun displayStatus() {
        logger.info {
            "Close Connections after uploading $rowCount files! " +
                    "(Small files in HBASE = $fileContentsInHbase; " +
                    "Large files  in HDFS = $fileContentsInHdfs)"
        }
    }

}
