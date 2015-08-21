package nl.knaw.dans.easy.ingest_flow

import java.io.File
import java.net.URL

import com.yourmediashelf.fedora.client.FedoraCredentials
import nl.knaw.dans.easy.fsrdb.FsRdbUpdater
import nl.knaw.dans.easy.ingest.EasyIngest
import nl.knaw.dans.easy.solr.EasyUpdateSolrIndex
import nl.knaw.dans.easy.stage.EasyStageDataset
import org.slf4j.LoggerFactory

import scala.util.Try

object EasyIngestFlow {
  val log = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]) {

    val fedoraCredentials = new FedoraCredentials(
      "http://deasy:8080/fedora", "fedoraAdmin", "fedoraAdmin")

    val sdo = new File("out/sdoSetDir")

    implicit val stageDatasetSettings = nl.knaw.dans.easy.stage.Settings(
      ownerId = "ownerId",
      bagStorageLocation = "http://localhost/bags",
      bagitDir = new File("test-resources/example-bag"),
      sdoSetDir = sdo,
      URN = "urn:nbn:nl:ui:13-1337-13",
      DOI = "10.1000/xyz123")

    implicit val ingestSettings = nl.knaw.dans.easy.ingest.Settings(
      fedoraCredentials, sdo)

    implicit val updateFsRdbSettings = nl.knaw.dans.easy.fsrdb.Settings(
      fedoraCredentials = fedoraCredentials,
      postgresURL = "postGresURL",
      datasetPid = "easy-dataset:39")

    implicit val updateSolrIndexSettings = nl.knaw.dans.easy.solr.Settings(
      fedoraCredentials = fedoraCredentials,
      solr = new URL("http://deasy:8080/solr/datasets/update"),
      dataset = "easy-dataset:39")

    for {
      _ <- Try { log.info("Staging dataset") }
      _ <- EasyStageDataset.run
      _ = log.info("Ingesting staged digital object into Fedora")
      pidDictionary <- EasyIngest.run
      _ = log.info("Updating PostgreSQL database")
      _ <- FsRdbUpdater.run
      _ = log.info("Updating Solr index")
      _ <- EasyUpdateSolrIndex.run
    } yield log.info("Finished")

  }
}
