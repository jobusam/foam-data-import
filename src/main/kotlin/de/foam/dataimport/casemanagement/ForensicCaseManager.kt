package de.foam.dataimport.casemanagement

import de.foam.dataimport.HBaseConnection
import de.foam.dataimport.TABLE_NAME_FORENSIC_DATA
import mu.KotlinLogging
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.client.Table
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter
import org.apache.hadoop.hbase.io.compress.Compression
import java.nio.charset.Charset

/**
 * @author jobusam
 * The ForensicCaseManager writes Metadata about the forensic case (case name, id, examiner)
 * into an HBASE Table. Also for every data import a new Forensic Exhibit is created in another
 * HBASE Table holding information about the exhibit (id, name, belonging case id,
 * import date and HDFS base directory). This case metadata will be accessed later for data processing.
 *
 **/

const val TABLE_NAME_FORENSIC_CASE = "forensicCase"
const val TABLE_NAME_FORENSIC_EXHIBIT = "forensicExhibit"
const val COLUMN_FAMILY_NAME_COMMON = "common"

class ForensicCaseManager() {

    private val logger = KotlinLogging.logger {}
    private val utf8 = Charset.forName("utf-8")

    /**
     * Create the HBASE tables "forensicCase" and "forensicExhibit" for
     * forensic case management
     */
    fun createForensicCaseManagementTables() {
        createTable(createForensicTableDescriptor(TABLE_NAME_FORENSIC_CASE))
        createTable(createForensicTableDescriptor(TABLE_NAME_FORENSIC_EXHIBIT))
    }

    private fun createForensicTableDescriptor(tableName: String): HTableDescriptor {
        val table = HTableDescriptor(TableName.valueOf(tableName))
        table.addFamily(HColumnDescriptor(COLUMN_FAMILY_NAME_COMMON)
                .setCompressionType(Compression.Algorithm.NONE))
        return table
    }

    /**
     * Display all created cases and imported Exhibits
     */
    fun displayCasesAndExhibits() {
        HBaseConnection.connection?.let { connection ->
            val caseTable = connection.getTable(TableName.valueOf(TABLE_NAME_FORENSIC_CASE))
            val exhibitTable = connection.getTable(TableName.valueOf(TABLE_NAME_FORENSIC_EXHIBIT))

            //Do a full table scan. But normally not many data entries are persisted in these tables
            val cases = caseTable.getScanner(Scan())
            val cfc = COLUMN_FAMILY_NAME_COMMON.toByteArray(utf8)

            val output = StringBuilder()
            output.append("\nList of available Forensic Cases and Exhibits:")

            cases.forEach { case ->
                val caseId = case.row.toString(utf8)
                val caseNumber:String? = case.getValue(cfc , FORENSIC_CASE_CASE_NUMBER.toByteArray(utf8))?.toString(utf8)
                val caseName:String? = case.getValue(cfc , FORENSIC_CASE_CASE_NAME.toByteArray(utf8))?.toString(utf8)
                val examiner:String? = case.getValue(cfc , FORENSIC_CASE_CASE_EXAMINER.toByteArray(utf8))?.toString(utf8)

                output.append("\n\n$caseId - Case: Number = <$caseNumber>, Name = <$caseName>, Examiner = <$examiner>\n")

                //FIXME: Try to cache the exhibit results and don't request it every case!
                exhibitTable.getScanner(Scan()).filter { exhibit ->
                    exhibit.row.toString(utf8).startsWith(case.row.toString(utf8))
                }.forEach { exhibit ->
                    val exhibitId = exhibit.row.toString(utf8)
                    val exhibitName:String? = exhibit.getValue(cfc , FORENSIC_EXHIBIT_EXHIBIT_NAME.toByteArray(utf8))?.toString(utf8)
                    val importDate:String? = exhibit.getValue(cfc , FORENSIC_EXHIBIT_IMPORT_DATE.toByteArray(utf8))?.toString(utf8)
                    val hdfsDirectory:String? = exhibit.getValue(cfc , FORENSIC_EXHIBIT_HDFS_EXHIBIT_DIRECTORY.toByteArray(utf8))?.toString(utf8)
                    output.append("\t$exhibitId - Exhibit: Name = <$exhibitName>, Import Date = <$importDate>, HDFS Directory = <$hdfsDirectory>\n")
                }

            }
            output.append("\n\n")
            System.out.print(output.toString())
            caseTable.close()
            exhibitTable.close()
        }
    }

    /**
     * Create a New Case. If the given ForensicCase already exists in database, the
     * existing case will be used.
     * Return a row key prefix that contains the case id and the exhibit id. This
     * row key prefix shall be used for importing data files and metadata
     */
    fun createNewCase(forensicCase: ForensicCase): String? {
        HBaseConnection.connection?.let { connection ->
            val caseTable = connection.getTable(TableName.valueOf(TABLE_NAME_FORENSIC_CASE))
            var caseId = getCaseId(forensicCase.caseNumber, caseTable)
            if (caseId != null) {
                logger.info { "Forensic case with the case number = ${forensicCase.caseNumber} already exists! Add the evidence to this case." }
            } else {
                logger.info { "Create a forensic case with content = $forensicCase" }
                caseId = getNextFreeRowKey(caseTable)
                caseTable.put(createPuts(forensicCase.toMap(), caseId))
                caseTable.close()
            }
            logger.info { "Create a forensic evidence with content = ${forensicCase.forensicExhibit}" }
            val exhibitTable = connection.getTable(TableName.valueOf(TABLE_NAME_FORENSIC_EXHIBIT))
            val exhibitId = "${caseId}_${getNextFreeRowKey(exhibitTable)}"
            //update hdfs base directory and add the exhibit id to store large files in separate exhibit folders
            val updatedExhibit = forensicCase.forensicExhibit.copy(
                    hdfsExhibitDirectory = forensicCase.forensicExhibit.hdfsExhibitDirectory.resolve(exhibitId))
            exhibitTable.put(createPuts(updatedExhibit.toMap(), exhibitId))
            exhibitTable.close()
            return exhibitId
        }
        return null
    }

    /**
     * Check for any case with the given case number and
     * return the row id (case id) of this case, or null otherwise
     */
    private fun getCaseId(caseNumber: String, table: Table): String? {
        val scanner = table.getScanner(COLUMN_FAMILY_NAME_COMMON.toByteArray(utf8), FORENSIC_CASE_CASE_NUMBER.toByteArray(utf8))
        val result = scanner.find {
            it
                    .getValue(COLUMN_FAMILY_NAME_COMMON.toByteArray(utf8), FORENSIC_CASE_CASE_NUMBER.toByteArray(utf8))
                    .toString(utf8) == caseNumber
        }
        return result?.row?.toString(utf8)

    }

    /**
     * The rows will be incremented. So this method returns the next free row key
     * from the given HBASE table.
     */
    private fun getNextFreeRowKey(table: Table): String {
        val scan = Scan()
        scan.filter = FirstKeyOnlyFilter()
        return table.getScanner(scan).count().toString()
    }

    private fun createPuts(values: Map<String, String>, row: String): List<Put> {
        return values.map {
            Put(row.toByteArray(utf8)).addColumn(COLUMN_FAMILY_NAME_COMMON.toByteArray(utf8),
                    it.key.toByteArray(utf8),
                    it.value.toByteArray(utf8))
        }
    }


    /**
     * Create the given table on HBASE.
     * Do nothing if the table already exists.
     */
    fun createTable(table: HTableDescriptor, splitTables:Boolean = false) {
        HBaseConnection.connection?.let { connection ->
            connection.admin.use { admin ->

                if (admin.tableExists(table.tableName)) {
                    logger.debug { "Reuse already created table ${table.tableName}!" }
                } else {
                    logger.debug { "Creating table ${table.tableName} in HBASE" }

                    if (splitTables){
                        val regions = 0 .. 29
                        val splitKeys = regions
                                .map { "%02d".format(it) }
                                .map { it.toByteArray(Charsets.UTF_8) }
                                .toTypedArray()
                        admin.createTable(table,splitKeys)
                    }else{
                        admin.createTable(table)
                    }
                    logger.debug { "Finished creation of table ${table.tableName}" }
                }
            }
        }
    }

    fun getAllHdfsDirectories():List<String>{
        HBaseConnection.connection?.let { connection ->
            val exhibitTable = connection.getTable(TableName.valueOf(TABLE_NAME_FORENSIC_EXHIBIT))
            val scanner = exhibitTable.getScanner(COLUMN_FAMILY_NAME_COMMON.toByteArray(utf8),
                    FORENSIC_EXHIBIT_HDFS_EXHIBIT_DIRECTORY.toByteArray(utf8))
            return scanner.map {
                it.getValue(COLUMN_FAMILY_NAME_COMMON.toByteArray(utf8), FORENSIC_EXHIBIT_HDFS_EXHIBIT_DIRECTORY.toByteArray(utf8))
                        .toString(utf8)
            }
        }
        return listOf()
    }

    /**
     * Delete all cases and exhibits.
     */
    fun deleteForensicData() {
        deleteTable(TABLE_NAME_FORENSIC_DATA)
        deleteTable(TABLE_NAME_FORENSIC_CASE)
        deleteTable(TABLE_NAME_FORENSIC_EXHIBIT)
    }

    /**
     * Delete the table with the given name on HBASE.
     * If the table doesn't exist than do nothing.
     */
    fun deleteTable(tableName: String) {
        HBaseConnection.connection?.let { connection ->
            connection.admin.use { admin ->
                val forensicTableName = TableName.valueOf(tableName)
                if (admin.tableExists(forensicTableName)) {
                    logger.debug { "Delete table $tableName" }
                    if (admin.isTableEnabled(forensicTableName)) {
                        admin.disableTable(forensicTableName)
                    }
                    admin.deleteTable(forensicTableName)
                } else {
                    logger.debug { "Table $tableName doesn't exists!" }
                }
            }
        }
    }

}