package de.foam.dataimport

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.core.subcommands
import com.github.ajalt.clikt.parameters.arguments.argument
import com.github.ajalt.clikt.parameters.arguments.convert
import com.github.ajalt.clikt.parameters.options.convert
import com.github.ajalt.clikt.parameters.options.flag
import com.github.ajalt.clikt.parameters.options.option
import de.foam.dataimport.casemanagement.ForensicCase
import de.foam.dataimport.casemanagement.ForensicCaseManager
import de.foam.dataimport.casemanagement.ForensicExhibit
import mu.KotlinLogging
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util.*
import javax.security.auth.login.AppConfigurationEntry
import javax.security.auth.login.Configuration
import kotlin.collections.HashMap


/**
 * @author jobusam
 *
 * Upload all files of the given directory into Apache Hadoop.
 * Also preserve the original file metadata from local fs.
 * Different approaches to preserve the file metadata will be implemented
 * and reviewed in this simple application.
 */


const val DEFAULT_HDFS_BASE_DIRECTORY = "/data/"

/**
 * Use kotlin-logging (Micro-Utils).
 * Define log level in simplelogger.properties files (see resources)
 */
private val logger = KotlinLogging.logger {}


fun main(args: Array<String>) {

    // addSecurityKerberos()
    ForensicDataImport().subcommands(Import(),Show()).main(args)

}

class ForensicDataImport : CliktCommand(){
    override fun run() = Unit
}

class Import : CliktCommand(help = "Import a forensic exhibit") {

    private val verbose: Boolean by option("-v", "--verbose", help = "enable verbose mode").flag()

    private val inputDirectory: Path by argument(
            help = "contains the path to local source directory that shall be imported into forensic analysis platform").convert { Paths.get(it) }

    private val hdfsBaseDirectory: Path? by option("-o", "--hdfsBaseDirectory",
            help = "contains the base directory in HDFS where large files will be stored. The default is $DEFAULT_HDFS_BASE_DIRECTORY").convert { Paths.get(it) }

    private val hbaseSiteXmlFile: Path? by option("-x", "--hbaseSiteXml",
            help = "contains file path to hbase-site.xml configuration file. If not given use localhost:2181 for connecting to Zookeeper").convert { Paths.get(it) }

    private val hdfsCoreXmlFile: Path? by option("-y", "--hdfsCoreXml",
            help = "contains file path to Hadoop core-site.xml configuration file. If not given use default hdfs uri hdfs://localhost:9000").convert { Paths.get(it) }

    private val caseNumber: String? by option("-c", "--caseNumber",
            help = "To which case this exhibit belongs")

    private val caseName: String? by option("-d", "--caseName",
            help = "Descriptive name of the case")

    private val examiner: String? by option("-e", "--examiner",
            help = "The name of the examiner")

    private val exhibitName: String? by option ("-f","--exhibitName",
            help = "The name of the exhibit (e.g: Windows System Image")

    override fun run() {
        logger.info { "Starting Forensic Data Import..." }
        logger.info {
            "Use following parameters:\n " +
                    "inputDirectory = $inputDirectory\n " +
                    "hdfsBaseDirectory = $hdfsBaseDirectory (optional)\n " +
                    "HBASE configuration path = $hbaseSiteXmlFile (optional)\n " +
                    "HDFS configuration path = $hdfsCoreXmlFile (optional)\n " +
                    "Examiner = $examiner (optional)\n " +
                    "Case Number = $caseNumber (optional)\n " +
                    "Case Name = $caseName (optional)\n " +
                    "Exhibit Name = $exhibitName (optional)\n " +
                    "Verbose = $verbose (optional)\n "
        }
        importDataVariantWithHbase()
        logger.info { "Forensic Data Import finished." }
    }

    /**
     * Upload data via HBASE JAVA API into Hadoop Cluster / HDFS.
     * Persist Metadata and small files into HBASE. Save large files
     * directly in HDFS. Therefore the hdfsDirectoryPath defines the location,
     * where large data files will be stored!
     */
    private fun importDataVariantWithHbase() {

        //Create a global HBASE Connection that will be used by other instances
        val hbaseConnection = HBaseConnection()
        hbaseConnection.createConnection(hbaseSiteXmlFile)

        val forensicExhibit = ForensicExhibit(exhibitName, Date().toString(),
                hdfsBaseDirectory ?: Paths.get(DEFAULT_HDFS_BASE_DIRECTORY))
        val forensicCase = ForensicCase(caseNumber?: UUID.randomUUID().toString(),
                forensicExhibit,caseName, examiner)
        val hdfsImport = HDFSDataImport(inputDirectory, forensicExhibit.hdfsExhibitDirectory, hdfsCoreXmlFile)
        val hbaseImport = HbaseDataImport(inputDirectory, hdfsImport, forensicCase)

        logger.info { "Upload files into HBASE and HDFS. Use Case Number ${forensicCase.caseNumber} for data import!" }
        val rootDirectory = inputDirectory.toFile()
        Files.walk(rootDirectory.toPath())
                .parallel() //Keep in mind this can cause other problems (see https://dzone.com/articles/think-twice-using-java-8)
                .peek { it?.let { logger.trace { it } } }
                .map { getFileMetadata(it, inputDirectory) }
                .peek { it?.let { logger.trace { it } } }
                .forEach { it?.let { hbaseImport.uploadFile(it) } }
        hbaseImport.displayStatus()

        // Using Kotlin rootDir causes problems with symbolic link directories
        //rootDirectory.walk(FileWalkDirection.TOP_DOWN)
        //.onNext

        //free resources
        hdfsImport.closeConnection()
        hbaseConnection.closeConnection()
    }
}

class Show : CliktCommand(help = "List forensic Cases") {

    private val hbaseSiteXmlFile: Path? by option("-x", "--hbaseSiteXml",
            help = "contains file path to hbase-site.xml configuration file. If not given use localhost:2181 for connecting to Zookeeper").convert { Paths.get(it) }

    override fun run() {
        logger.info { "Manage Forensic Cases" }
        //Create a global HBASE Connection that will be used by other instances
        val hbaseConnection = HBaseConnection()
        hbaseConnection.createConnection(hbaseSiteXmlFile)

        val forensicCaseManager = ForensicCaseManager()
        forensicCaseManager.displayCasesAndExhibits()
        hbaseConnection.closeConnection()
    }
}


/**
 * This method will set common Kerberos configurations.
 * The configuration is not required if the data import will be executed
 * on a local hadoop cluster instance because the system provides a proper
 * configuration via system variables.
 * So the at least the configuration has to be set / checked
 * if the data import will be executed on an remote client that doesn't have any Kerberos access
 * configured!
 */
fun addSecurityKerberos() {
    // Following system properties are not required if the app is executed on local hadoop instance!
    // At least the values will be retrieved automatically from system conf /etc/krb5.conf
    // See https://docs.oracle.com/javase/8/docs/technotes/guides/security/jgss/tutorials/KerberosReq.html
    System.setProperty("java.security.krb5.realm", "REALM_NAME")
    System.setProperty("java.security.krb5.kdc", "HADOOP_SERVER_NAME")

    // On a Hadoop cluster instance following is not required.
    Configuration.setConfiguration(object : Configuration() {
        override fun getAppConfigurationEntry(p0: String?): Array<AppConfigurationEntry> {

            val properties = HashMap<String, String>()
            properties["client"] = "true"

            val configEntry = AppConfigurationEntry("com.sun.security.auth.module.Krb5LoginModule",
                    AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, properties)
            return Arrays.asList(configEntry).toTypedArray()
        }
    })
}