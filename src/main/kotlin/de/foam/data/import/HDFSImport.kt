package de.foam.data.import

import mu.KotlinLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.security.UserGroupInformation
import java.nio.file.Path

/**
 * Define the default URI for accessing HDFS.
 * Will be used in case there is no configuration file given during startup (via cli parameter)
 */
const val HADOOP_DEFAULT_FS = "hdfs://localhost:9000"

/**
 * @author jobusam
 * Upload a file into HDFS. Therefore use the HDFS JAVA Client API.
 */
class HDFSImport(private val inputDirectory: Path, private val hdfsBaseDirectory: Path, hdfsCoreXML: Path?) {

    private val logger = KotlinLogging.logger {}

    private var filesystem: FileSystem? = null

    init {
        val conf = createStandardConf(hdfsCoreXML)
        //val conf = createKerberosConf()
        filesystem = FileSystem.get(conf)
        logger.info { "Reset base directory $hdfsBaseDirectory and delete content." }
        filesystem?.delete(org.apache.hadoop.fs.Path(hdfsBaseDirectory.toString()), true)
        filesystem?.mkdirs(org.apache.hadoop.fs.Path(hdfsBaseDirectory.toString()))
    }

    /**
     * Create a default configuration that can be used in a unsecured cluster.
     * No Kerberos support!
     */
    private fun createStandardConf(hdfsCoreXML: Path?): Configuration {
        val conf = Configuration()
        if (hdfsCoreXML != null) {
            conf.addResource(org.apache.hadoop.fs.Path(hdfsCoreXML.toUri()))
            logger.info { "Use configuration file $hdfsCoreXML to connect to HDFS." }
        } else {
            conf.set("fs.defaultFS", HADOOP_DEFAULT_FS)
            logger.info { "Use fs.defaultFS = $HADOOP_DEFAULT_FS" }
        }
        return conf
    }

    /**
     * If Kerberos is activated in Hadoop Cluster following method must be used to
     * crate a proper configuration for connecting to HDFS and HBASE (see init method).
     * FIXME: provide proper param support for conf files in cli call!
     */
    private fun createKerberosConf(): Configuration {
        val conf = Configuration()
        conf.addResource(org.apache.hadoop.fs.Path("/etc/hadoop/conf/hdfs-site.xml"))
        conf.addResource(org.apache.hadoop.fs.Path("/etc/hadoop/conf/core-site.xml"))

        //conf.set("hadoop.security.authentication", "kerberos")
        //Following code must be executed at least one time per jvm instance!
        UserGroupInformation.setConfiguration(conf)
        val user = System.getProperty("user.name")
        val keytabPath = "/etc/security/keytabs/$user.keytab"
        logger.info { "Current user is $user. Use Kerberos Keytab $keytabPath" }
        UserGroupInformation.loginUserFromKeytab(user, keytabPath)
        return conf
    }

    fun uploadFileIntoHDFS(relativeFilePath: String, targetFileName: String) {
        val inputFilePath = org.apache.hadoop.fs.Path(inputDirectory.resolve(relativeFilePath).toUri())
        val targetHDFSFilePath = org.apache.hadoop.fs.Path(hdfsBaseDirectory.resolve(targetFileName).toString())
        filesystem?.copyFromLocalFile(inputFilePath, targetHDFSFilePath)
        logger.trace { "HDFS Upload <$inputFilePath> to HDFS:$targetHDFSFilePath" }
    }

    fun getHDFSBaseDirectory(): Path = this.hdfsBaseDirectory

    fun closeConnection() {
        logger.info { "Close Connection to HDFS" }
        filesystem?.close()
        filesystem = null
    }
}