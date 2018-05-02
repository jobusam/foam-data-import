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
import java.util.stream.Collectors
import kotlin.reflect.full.memberProperties


/**
 * @author jobusam
 * Upload data into HBASE. Use HBASE Java Client API to upload the data
 *
 * Advantages of this solution:
 * - File metadata is stored on Data Nodes
 * - Small files are directly persisted in HBASE (no HDFS metadata overhead)
 * - Better handling of symbolic links
 *   - only entry in hbase will be created
 *   - avoid resolution and upload of character devices!
 *
 * Disadvantages:
 * - Files aren't directly accessible in HDFS
 * - For displaying directory hierarchy and files an own viewer must be implemented.
 * - The original filesystem structure is not directly persisted in HDFS
 *
 * TODO:
 * - Compile Hadoop native lib (libhadoop.so) for x64 Fedora to improve performance!
 * - Overall performance must be improved. The data import needs to much time!
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

class HbaseImport(private val inputDirectory: Path, hbaseSiteXML: Path?) {

    private val logger = KotlinLogging.logger {}
    private val config = HBaseConfiguration.create()

    private var connection : Connection? = null
    private var rowCount = 0

    // for statistics only
    private var fileContentsInHBASE = 0

    init {
        logger.info { "Try to connect to HBASE..." }
        val url : URL? = hbaseSiteXML?.toUri()?.toURL() ?: HbaseImport::class.java.getResource("/hbase-site-client.xml")
        url?.let {
            logger.info{ "Use configuration file $url" }
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
            if (admin.isTableEnabled(table.tableName)){
                admin.disableTable(table.tableName)
            }
            admin.deleteTable(table.tableName)
        }
        admin.createTable(table)
    }

    /**
     * Create Tables for persisting files and file metadata in HBASE and HDFS
     */
    fun createTables(){
        logger.info { "Create Tables in Hbase in case they are not available" }
        connection?.let { connection ->
            connection.admin.use { admin ->

                val table = HTableDescriptor(TableName.valueOf(TABLE_NAME_FORENSIC_DATA))
                table.addFamily(HColumnDescriptor(COLUMN_FAMILY_NAME_METADATA).setCompressionType(Compression.Algorithm.NONE))
                table.addFamily(HColumnDescriptor(COLUMN_FAMILY_NAME_CONTENT).setCompressionType(Compression.Algorithm.NONE))

                logger.info { "Creating table $table" }
                createOrOverwrite(admin, table)
                logger.info { "Finished creation of table $table done" }
            }
        }
    }

    /**
     * Upload a single file content and the given metadata into HBASE
     */
    fun uploadFile(fileMetadata: FileMetadata) {
        connection?.let { connection ->
            logger.trace { "Upload Metadata of file ${inputDirectory.resolve(fileMetadata.relativeFilePath)}" }
            val table = connection.getTable(TableName.valueOf(TABLE_NAME_FORENSIC_DATA))
            table.put(createPuts(fileMetadata))
        }
    }

    /**
     * create Put objects to import every metadata into a single column
     */
    private fun createPuts(fileMetadata: FileMetadata): List<Put>{
        val row = "row"+rowCount++
        val utf8 = Charset.forName("utf-8")

        //TODO: save file timestamps in seperate columns!
        val puts = FileMetadata::class.memberProperties.stream().map {
            Put(row.toByteArray(utf8)).addColumn(COLUMN_FAMILY_NAME_METADATA.toByteArray(utf8),
                    it.name.toByteArray(utf8),
                    "${it.get(fileMetadata)}".toByteArray(utf8))
        }.collect(Collectors.toList())

        //TODO: upload large files directly into HDFS!
        //raw data put
        // The default maximum keyvalue size is 10 MB (10485760 Bytes)
        // 10485660 (100 Bytes reserved for key itself ;) )
        // Keep in mind. The default size in Hortonworks Data platform is 1 MB and should be increased at least to 10 MB!
        if (FileType.DATA_FILE == fileMetadata.fileType &&fileMetadata.fileSize != null && 10485660 >= fileMetadata.fileSize){
            val absolutePath = inputDirectory.resolve(fileMetadata.relativeFilePath)
            puts.add(Put(row.toByteArray(utf8)).addColumn(COLUMN_FAMILY_NAME_CONTENT.toByteArray(utf8),
                    "fileContent".toByteArray(utf8),
                    Files.readAllBytes(absolutePath)))
            fileContentsInHBASE++
        }
        return puts
    }

    /**
     * Close any open connection to HBASE. Call this method to release all resources of this instance!
     */
    fun closeConnection(){
        logger.info { "Close Connections after uploading $rowCount files and $fileContentsInHBASE file contents into HBASE!" }
        connection?.close()
        connection = null
    }

}
