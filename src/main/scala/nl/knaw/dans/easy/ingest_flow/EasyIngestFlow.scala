package nl.knaw.dans.easy.ingest_flow

import java.io.File
import java.net.URL

import com.yourmediashelf.fedora.client.FedoraCredentials
import nl.knaw.dans.easy.fsrdb.FsRdbUpdater
import nl.knaw.dans.easy.ingest.EasyIngest
import nl.knaw.dans.easy.ingest.EasyIngest.PidDictionary
import nl.knaw.dans.easy.solr.EasyUpdateSolrIndex
import nl.knaw.dans.easy.stage.EasyStageDataset
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

object EasyIngestFlow {
  val log = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]) {
    implicit val fedoraCreds = new FedoraCredentials(
      "http://deasy:8080/fedora", "fedoraAdmin", "fedoraAdmin")
    run(new File("out/sdoSetDir"))
  }

  def run(sdo: File)(implicit fedoraCreds: FedoraCredentials): Try[Unit] = {
    for {
      _ <- stageDataset(sdo)
      pidDictionary <- ingestDataset(sdo)
      datasetPid <- getDatasetPid(pidDictionary)
      _ <- updateFsRdb(datasetPid)
      _ <- updateSolr(datasetPid)
    } yield log.info("Finished")
  }

  def stageDataset(sdo: File): Try[Unit] = {
    log.info("Staging dataset")
    implicit val stageDatasetSettings = nl.knaw.dans.easy.stage.Settings(
      ownerId = "ownerId",
      bagStorageLocation = "http://localhost/bags",
      bagitDir = new File("test-resources/example-bag"),
      sdoSetDir = sdo,
      URN = "urn:nbn:nl:ui:13-1337-13",
      DOI = "10.1000/xyz123")
    EasyStageDataset.run
  }

  def ingestDataset(sdo: File)(implicit fedoraCreds: FedoraCredentials): Try[PidDictionary] = {
    log.info("Ingesting staged digital object into Fedora")
    implicit val ingestSettings = nl.knaw.dans.easy.ingest.Settings(fedoraCreds, sdo)
    EasyIngest.run
  }

  def updateFsRdb(datasetPid: String)(implicit fedoraCreds: FedoraCredentials): Try[Unit] = {
    log.info("Updating PostgreSQL database")
    implicit val updateFsRdbSettings = nl.knaw.dans.easy.fsrdb.Settings(
      fedoraCredentials = fedoraCreds,
      postgresURL = "postGresURL",
      datasetPid = datasetPid)
    FsRdbUpdater.run
  }

  def updateSolr(datasetPid: String)(implicit fedoraCreds: FedoraCredentials): Try[Unit] = {
    log.info("Updating Solr index")
    implicit val updateSolrIndexSettings = nl.knaw.dans.easy.solr.Settings(
      fedoraCredentials = fedoraCreds,
      solr = new URL("http://deasy:8080/solr/datasets/update"),
      dataset = datasetPid)
    EasyUpdateSolrIndex.run
  }

  def getDatasetPid(pidDictionary: PidDictionary): Try[String] = {
    pidDictionary.values.find(_.startsWith("easy-dataset")) match {
      case Some(pid) => Success(pid)
      case None => Failure(new RuntimeException("SDO-set didn't contain a dataset object."))
    }
  }

}
