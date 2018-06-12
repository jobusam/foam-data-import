package de.foam.data.import

import mu.KotlinLogging
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util.Arrays
import javax.security.auth.login.AppConfigurationEntry
import javax.security.auth.login.Configuration


/**
 * @author jobusam
 *
 * Upload all files of the given directory into Apache Hadoop.
 * Also preserve the original file metadata from local fs.
 * Different approaches to preserve the file metadata will be implemented
 * and reviewed in this simple application.
 */

/**
 * Use kotlin-logging (Micro-Utils).
 * Define log level in simplelogger.properties files (see resources)
 */
private val logger = KotlinLogging.logger {}


fun main(args: Array<String>) {
    logger.info { "Starting Forensic Data Import..." }
    // addSecurityKerberos()
    // dataImportVariantWithCLICommand(args)
    dataImportVariantWithHBase(args)
    logger.info { "Forensic Data Import finished." }
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
            properties.put("client", "true")

            val configEntry = AppConfigurationEntry("com.sun.security.auth.module.Krb5LoginModule",
                    AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, properties)
            return Arrays.asList(configEntry).toTypedArray()
        }
    })
}

/**
 * Upload data via HBASE JAVA API into Hadoop Cluster / HDFS.
 * Persist Metadata and small files into HBASE. Save large files
 * directly in HDFS. Therefore the hdfsDirectoryPath defines the location,
 * where large data files will be stored!
 */
fun dataImportVariantWithHBase(args: Array<String>) {

    val inputDirectory: Path
    val hdfsDirectoryPath: Path
    val hbaseSiteXML: Path?
    val hdfsCoreXML: Path?
    when (args.size) {
        2 -> {
            inputDirectory = Paths.get(args[0])
            hdfsDirectoryPath = Paths.get(args[1])
            hbaseSiteXML = null
            hdfsCoreXML = null
        }
        4 -> {
            inputDirectory = Paths.get(args[0])
            hdfsDirectoryPath = Paths.get(args[1])
            hbaseSiteXML = Paths.get(args[2])
            hdfsCoreXML = Paths.get(args[3])
        }
        else -> {
            logger.error {
                "Wrong arguments! Follow syntax is provided:\n" +
                        "LOCAL_SOURCE_DIR REMOTE_HDFS_TARGET_DIR [HBASE_SITE_XML] [HDFS_CORE_XML]\n" +
                        "Use absolute paths for all arguments.\n" +
                        "Example: java -jar data.import-1.0-SNAPSHOT-capsule.jar /home/hdtest/testdata/image /user/hdtest/image " +
                        "/etc/hbase/conf/hbase-site.xml /etc/hadoop/conf/core-site.xml"
            }
            return
        }
    }
    logger.info {
        "Use following parameters:\n inputDirectory = $inputDirectory\n " +
                "hdfsDirectory = $hdfsDirectoryPath\n " +
                "HBASE configuration path = $hbaseSiteXML (optional)\n" +
                "HDFS configuration path = $hdfsCoreXML (optional)"
    }

    val hdfsImport = HDFSImport(inputDirectory, hdfsDirectoryPath, hdfsCoreXML)
    val hbaseImport = HbaseImport(inputDirectory, hbaseSiteXML, hdfsImport)

    logger.info { "Create Tables in HBASE" }
    hbaseImport.createTables()

    logger.info { "Upload files into HBASE and HDFS" }
    val rootDirectory = inputDirectory.toFile()
    Files.walk(rootDirectory.toPath())
            .parallel() //Keep in mind this can cause other problems (see https://dzone.com/articles/think-twice-using-java-8)
            .peek { it?.let { logger.trace { it } } }
            .map { getFileMetadata(it, inputDirectory) }
            .peek { it?.let { logger.trace { it } } }
            .forEach { it?.let { hbaseImport.uploadFile(it) } }

    // Using Kotlin rootDir causes problems with symbolic link directories
    //rootDirectory.walk(FileWalkDirection.TOP_DOWN)
    //.onNext

    //free resources
    hbaseImport.closeConnection()
    hdfsImport.closeConnection()
}

/**
 * Upload data into local HDFS. Use shell "hdfs" command for upload files and metadata.
 * See class HdfsImportCli for further details on this implementation
 */
fun dataImportVariantWithCLICommand(args: Array<String>) {

    val inputDirectory: Path
    val hdfsDirectoryPath: Path
    val hadoopHome: Path?
    when (args.size) {
        2 -> {
            inputDirectory = Paths.get(args[0])
            hdfsDirectoryPath = Paths.get(args[1])
            hadoopHome = null
        }
        3 -> {
            inputDirectory = Paths.get(args[0])
            hdfsDirectoryPath = Paths.get(args[1])
            hadoopHome = Paths.get(args[2])
        }
        else -> {
            logger.error {
                "Wrong arguments! Follow syntax is provided:\n" +
                        "LOCAL_SOURCE_DIR HDFS_TARGET_DIR [HDFS_HOME]\n" +
                        "Use absolute paths for all arguments. "
            }
            return
        }
    }


    logger.info {
        "Data from input data directory <$inputDirectory>" +
                " will be uploaded to hdfs target directory <$hdfsDirectoryPath>"
    }

    val hdfsImportCli = HdfsImportCli(hadoopHome, hdfsDirectoryPath, inputDirectory)

    val uploaded = hdfsImportCli.uploadContentToHDFS()
    if (!uploaded) {
        logger.error { "File upload of directory $inputDirectory failed. Stop Forensic Data Import!" }
        return
    }

    val rootDirectory = inputDirectory.toFile()
    rootDirectory.walk(FileWalkDirection.TOP_DOWN)
            .map { getFileMetadata(it.toPath(), inputDirectory) }
            .onEach { it?.let { logger.trace { it } } }
            .forEach { it?.let { hdfsImportCli.uploadFileMetadata(it) } }

    hdfsImportCli.waitForExecution()
}


