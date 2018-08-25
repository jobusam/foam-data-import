package de.foam.dataimport

import mu.KotlinLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.security.UserGroupInformation
import java.nio.file.Path

class HdfsConnection(hdfsCoreXML: Path?){

    private val logger = KotlinLogging.logger {}

    companion object {
        var filesystem: FileSystem? = null
    }

    init {
        createConnection(hdfsCoreXML)
    }

    fun createConnection( hdfsCoreXML: Path?){
        val conf = createStandardConf(hdfsCoreXML)
        //val conf = createKerberosConf()
        filesystem = FileSystem.get(conf)
    }

    fun closeConnection(){
        logger.info { "Close Connection to HDFS" }
        filesystem?.close()
        filesystem = null
    }

    /**
     * Create a default configuration that can be used in a unsecured cluster.
     * No Kerberos support!
     */
    private fun createStandardConf(hdfsCoreXML: Path?): Configuration {
        logger.info { "Connect to HDFS..." }
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
}