package nl.knaw.dans.easy.ingest_flow

import java.io.File
import java.net.URL

import com.yourmediashelf.fedora.client.FedoraCredentials
import nl.knaw.dans.easy._
import nl.knaw.dans.easy.fsrdb.FsRdbUpdater
import nl.knaw.dans.easy.ingest.EasyIngest
import nl.knaw.dans.easy.ingest.EasyIngest.PidDictionary
import nl.knaw.dans.easy.solr.EasyUpdateSolrIndex
import nl.knaw.dans.easy.stage.EasyStageDataset
import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}
import scalaj.http.Http

object EasyIngestFlow {
  val log = LoggerFactory.getLogger(getClass)

  case class Settings(fedoraCredentials: FedoraCredentials,
                      numSyncTries: Int,
                      syncDelay: Long,
                      ownerId: String,
                      bagStorageLocation: String,
                      bagitDir: String,
                      sdoSetDir: String,
                      DOI: String,
                      postgresURL: String,
                      solr: String,
                      pidgen: String)

  def main(args: Array[String]) {

    implicit val settings = Settings(
      fedoraCredentials = new FedoraCredentials("http://deasy:8080/fedora", "fedoraAdmin", "fedoraAdmin"),
      numSyncTries = 10,
      syncDelay = 3000,
      ownerId = "georgi",
      bagStorageLocation = "http://localhost/bags",
      bagitDir = "test-resources/example-bag",
      sdoSetDir = "out/sdoSetDir",
      DOI = "10.1000/xyz123",
      postgresURL = "jdbc:postgresql://deasy:5432/easy_db?user=easy_webui&password=easy_webui",
      solr = "http://deasy:8080/solr/datasets/update",
      pidgen = "http://deasy:8082/pids?type=urn"
    )

    run(new File("out/sdoSetDir")) match {
      case Success(datasetPid) => log.info(s"Finished, dataset pid: $datasetPid")
      case Failure(e) => throw e
    }
  }

  def run(sdo: File)(implicit s: Settings): Try[String] = {
    for {
      urn <- requestURN()
      _ <- stageDataset(sdo, urn)
      pidDictionary <- ingestDataset(sdo)
      datasetPid <- getDatasetPid(pidDictionary)
      _ <- waitForFedoraSync(datasetPid, pidDictionary, numTries = 10, delayMillis = 3000)
      _ <- updateFsRdb(datasetPid)
      _ <- updateSolr(datasetPid)
    } yield datasetPid
  }

  def stageDataset(sdo: File, urn: String)(implicit s: Settings): Try[Unit] = {
    log.info("Staging dataset")
    EasyStageDataset.run(stage.Settings(
      ownerId = s.ownerId,
      bagStorageLocation = s.bagStorageLocation,
      bagitDir = new File(s.bagitDir),
      sdoSetDir = sdo,
      URN = urn,
      DOI = s.DOI))
  }

  def ingestDataset(sdo: File)(implicit s: Settings): Try[PidDictionary] = {
    log.info("Ingesting staged digital object into Fedora")
    EasyIngest.run(ingest.Settings(s.fedoraCredentials, sdo))
  }

  def updateFsRdb(datasetPid: String)(implicit s: Settings): Try[Unit] = {
    log.info("Updating PostgreSQL database")
    FsRdbUpdater.run(fsrdb.Settings(
      fedoraCredentials = s.fedoraCredentials,
      postgresURL = s.postgresURL,
      datasetPid = datasetPid))
  }

  def updateSolr(datasetPid: String)(implicit s: Settings): Try[Unit] = {
    log.info("Updating Solr index")
    EasyUpdateSolrIndex.run(solr.Settings(
      fedoraCredentials = s.fedoraCredentials,
      solr = new URL(s.solr),
      dataset = datasetPid))
  }

  def getDatasetPid(pidDictionary: PidDictionary): Try[String] = {
    pidDictionary.values.find(_.startsWith("easy-dataset")) match {
      case Some(pid) => Success(pid)
      case None => Failure(new RuntimeException("SDO-set didn't contain a dataset object."))
    }
  }

  def requestURN()(implicit s: Settings): Try[String] =
    Try {
      Http(s.pidgen)
        .timeout(connTimeoutMs = 10000, readTimeoutMs = 50000)
        .postForm.asString
    }.flatMap(r =>
      if (r.code == 200) {
        val urn = r.body
        log.info(s"Requested URN: $urn")
        Success(urn)
      } else Failure(new RuntimeException(s"PID Generator failed: ${r.body}")))

  def waitForFedoraSync(datasetPid: String, pidDictionary: PidDictionary, numTries: Int, delayMillis: Long)(implicit s: Settings): Try[Unit] = {
    val expectedPids = pidDictionary.values.toSet - datasetPid
    @tailrec def loop(n: Int): Try[Unit] = {
      log.info(s"Check whether Fedora is synced. Tries left: $n.")
      if (n <= 0)
        Failure(new RuntimeException(s"Fedora didn't sync in time. Dataset: $datasetPid. Expected pids: $expectedPids"))
      else
        queryPids(datasetPid).map(pids => pids.size == expectedPids.size && pids.toSet == expectedPids) match {
          case Success(true) => Success(Unit)
          case _ =>
            Thread.sleep(delayMillis)
            loop(n-1)
        }
    }
    loop(numTries)
  }

  def queryPids(datasetPid: String)(implicit s: Settings): Try[List[String]] = Try {
    val url = s"${s.fedoraCredentials.getBaseUrl}/risearch"
    val response = Http(url)
      .timeout(connTimeoutMs = 10000, readTimeoutMs = 50000)
      .param("type", "tuples")
      .param("lang", "sparql")
      .param("format", "CSV")
      .param("query",
        s"""
           |select ?s
           |from <#ri>
           |where { ?s <http://dans.knaw.nl/ontologies/relations#isSubordinateTo> <info:fedora/$datasetPid> . }
        """.stripMargin)
      .asString
    if (response.code != 200)
      throw new RuntimeException(s"Failed to query fedora resource index ($url), response code: ${response.code}")
    response.body.lines.toList.drop(1)
      .map(_.replace("info:fedora/", ""))
  }

}
