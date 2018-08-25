package de.foam.dataimport.casemanagement

import java.nio.file.Path


const val FORENSIC_CASE_CASE_NUMBER = "caseNumber"
const val FORENSIC_CASE_CASE_NAME = "caseName"
const val FORENSIC_CASE_CASE_EXAMINER = "examiner"

const val FORENSIC_EXHIBIT_EXHIBIT_NAME = "exhibitName"
const val FORENSIC_EXHIBIT_IMPORT_DATE = "importDate"
const val FORENSIC_EXHIBIT_HDFS_EXHIBIT_DIRECTORY = "hdfsExhibitDirectory"

/**
 * @author jobusam
 * Data class for a forensic case.
 * In general a ForensicCase can have multiple ForensicExhibits!
 */
data class ForensicCase(val caseNumber: String, val forensicExhibit:ForensicExhibit, val caseName: String?, val examiner: String?) {

    fun toMap(): Map<String, String> {
        val values = mutableMapOf<String, String>()
        values[FORENSIC_CASE_CASE_NUMBER] = this.caseNumber
        caseName?.let { values[FORENSIC_CASE_CASE_NAME] = it }
        examiner?.let { values[FORENSIC_CASE_CASE_EXAMINER] = it }
        return values
    }
}

/**
 * Data class for a forensic exhibit.
 */
data class ForensicExhibit(val exhibitName: String?, val importDate: String?, val hdfsExhibitDirectory: Path) {
    fun toMap(): Map<String, String> {
        val values = mutableMapOf<String, String>()
        exhibitName?.let { values[FORENSIC_EXHIBIT_EXHIBIT_NAME] = it }
        importDate?.let { values[FORENSIC_EXHIBIT_IMPORT_DATE] = it }
        values[FORENSIC_EXHIBIT_HDFS_EXHIBIT_DIRECTORY] = this.hdfsExhibitDirectory.toString()
        return values
    }
}