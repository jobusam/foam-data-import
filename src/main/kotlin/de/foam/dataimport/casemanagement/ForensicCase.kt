package de.foam.dataimport.casemanagement

import java.nio.file.Path

/**
 * @author jobusam
 * Data class for a forensic case.
 * In general a ForensicCase can have multiple ForensicExhibits!
 */
data class ForensicCase(val caseNumber: String, val caseName: String?, val examiner: String?) {

    fun toMap(): Map<String, String> {
        val values = mutableMapOf<String, String>()
        values["caseNumber"] = this.caseNumber
        caseName?.let { values["caseName"] = it }
        examiner?.let { values["examiner"] = it }
        return values
    }
}

/**
 * Data class for a forensic exhibit.
 */
data class ForensicExhibit(val exhibitName: String?, val caseNumber: String,
                           val importDate: String?, val hdfsBaseDirectory: Path) {
    fun toMap(): Map<String, String> {
        val values = mutableMapOf<String, String>()
        exhibitName?.let { values["exhibitName"] = it }
        importDate?.let { values["importDate"] = it }
        values["hdfsBaseDirectory"] = this.hdfsBaseDirectory.toString()
        return values
    }
}