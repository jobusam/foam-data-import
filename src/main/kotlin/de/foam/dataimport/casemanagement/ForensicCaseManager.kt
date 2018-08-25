package de.foam.dataimport.casemanagement

import mu.KotlinLogging
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Connection
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

class ForensicCaseManager(private val getConnection: () -> Connection?) {

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
     * Delete the HBASE tables "forensicCase" and "forensicExhibit"
     */
    fun deleteForensicCaseManagementTables() {
        deleteTable(TABLE_NAME_FORENSIC_CASE)
        deleteTable(TABLE_NAME_FORENSIC_EXHIBIT)
    }

    /**
     * Create a New Case. If the given ForensicCase already exists in database, the
     * existing case will be used.
     * Return a row key prefix that contains the case id and the exhibit id. This
     * row key prefix shall be used for importing data files and metadata
     */
    fun createNewCase(forensicCase: ForensicCase, forensicExhibit: ForensicExhibit): String? {
        getConnection()?.let { connection ->
            val cases = connection.getTable(TableName.valueOf(TABLE_NAME_FORENSIC_CASE))
            var caseId = getCaseId(forensicCase.caseNumber, cases)
            if (caseId != null) {
                logger.info { "A forensic case with the case number = ${forensicCase.caseNumber} already exists! Add the evidence to this case." }
            } else {
                logger.info { "Create a forensic case with content = $forensicCase" }
                caseId = getNextFreeRowKey(cases)
                cases.put(createPuts(forensicCase.toMap(), caseId))
            }
            logger.info { "Create a forensic evidence with content = $forensicExhibit" }
            val exhibits = connection.getTable(TableName.valueOf(TABLE_NAME_FORENSIC_EXHIBIT))
            val exhibitId = "${caseId}_${getNextFreeRowKey(exhibits)}"
            exhibits.put(createPuts(forensicExhibit.toMap(), exhibitId))
            return exhibitId
        }
        return null
    }

    /**
     * Check for any case with the given case number and
     * return the row id (case id) of this case, or null otherwise
     */
    private fun getCaseId(caseNumber: String, table: Table): String? {
        val scanner = table.getScanner(COLUMN_FAMILY_NAME_COMMON.toByteArray(utf8), "caseNumber".toByteArray(utf8))
        val result = scanner.find {
            it
                    .getValue(COLUMN_FAMILY_NAME_COMMON.toByteArray(utf8), "caseNumber".toByteArray(utf8))
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
    fun createTable(table: HTableDescriptor) {
        getConnection()?.let { connection ->
            connection.admin.use { admin ->

                if (admin.tableExists(table.tableName)) {
                    logger.debug { "Reuse already created table ${table.tableName}!" }
                } else {
                    logger.debug { "Creating table ${table.tableName} in HBASE" }

                    admin.createTable(table)
                    logger.debug { "Finished creation of table ${table.tableName}" }
                }
            }
        }
    }

    /**
     * Delete the table with the given name on HBASE.
     * If the table doesn't exist than do nothing.
     */
    fun deleteTable(tableName: String) {
        getConnection()?.let { connection ->
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