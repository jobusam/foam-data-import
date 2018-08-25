package de.foam.dataimport

import mu.KotlinLogging
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.HBaseAdmin
import java.net.URL
import java.nio.file.Path

internal class HBaseConnection (){

    private val logger = KotlinLogging.logger {}

    companion object {
        var connection: Connection? = null
    }

    fun createConnection(hbaseSiteXML: Path?) {
        logger.info { "Try to connect to HBASE..." }
        val url: URL? = hbaseSiteXML?.toUri()?.toURL()
                ?: HbaseDataImport::class.java.getResource("/hbase-site-client.xml")
        url?.let {
            logger.info { "Use configuration file $url" }
            val config = HBaseConfiguration.create()
            config.addResource(org.apache.hadoop.fs.Path(url.path))
            HBaseAdmin.checkHBaseAvailable(config)
            connection = ConnectionFactory.createConnection(config)
        }
    }

    fun closeConnection(){
        connection?.close()
        connection = null
    }
}
