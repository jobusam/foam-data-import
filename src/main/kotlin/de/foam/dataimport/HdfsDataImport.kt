package de.foam.dataimport

import mu.KotlinLogging
import org.apache.hadoop.fs.FSError
import java.io.IOException
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
class HDFSDataImport(private val inputDirectory: Path, private val hdfsBaseDirectory: Path) {

    private val logger = KotlinLogging.logger {}

    //Only default backup if #createHDFSExhibitDiretory was not called!
    private var hdfsExhibitDirectory: Path? = hdfsBaseDirectory

    init {
        val hdfsPath = org.apache.hadoop.fs.Path(hdfsBaseDirectory.toString())
        if (HdfsConnection.filesystem?.exists(hdfsPath) == true)
            logger.info { "Base directory $hdfsBaseDirectory already exist." }
        else {
            logger.info { "Create a new Base Directory $hdfsBaseDirectory" }
            HdfsConnection.filesystem?.mkdirs(hdfsPath)
        }
    }

    fun uploadFileIntoHDFS(relativeFilePath: String, targetFileName: String) {
        val inputFilePath = org.apache.hadoop.fs.Path(inputDirectory.resolve(relativeFilePath).toUri())
        val targetHDFSFilePath = org.apache.hadoop.fs.Path((hdfsExhibitDirectory
                ?: hdfsBaseDirectory).resolve(targetFileName).toString())
        logger.trace { "HDFS Upload <$inputFilePath> to HDFS:$targetHDFSFilePath" }
        try {
            HdfsConnection.filesystem?.copyFromLocalFile(inputFilePath, targetHDFSFilePath)
        } catch (e: IOException){
            logger.error { " IOException: Can't upload file <$inputFilePath> to HDFS:$targetHDFSFilePath" }
        } catch (fsError: FSError){
            logger.error { "FS Error: Can't upload file <$inputFilePath> to HDFS:$targetHDFSFilePath" }
        }
    }

    fun createHDFSExhibitDiretory(exhibitID: String) {
        hdfsExhibitDirectory = hdfsBaseDirectory.resolve(exhibitID)
        val hdfsCasePath = org.apache.hadoop.fs.Path(hdfsExhibitDirectory.toString())
        if (HdfsConnection.filesystem?.exists(hdfsCasePath) == true)
            logger.warn {
                "Caution: Case directory $hdfsExhibitDirectory already exist " +
                        "and will be used for file upload. But normally this directory shouldn't exist."
            }
        else {
            logger.info { "Create a new Case Directory $hdfsExhibitDirectory" }
            HdfsConnection.filesystem?.mkdirs(hdfsCasePath)
        }
    }

    fun getHDFSExhibitDirectory(): Path = this.hdfsExhibitDirectory ?: hdfsBaseDirectory
}