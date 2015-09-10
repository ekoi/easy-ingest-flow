/**
 * *****************************************************************************
 * Copyright 2015 DANS - Data Archiving and Networked Services
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ****************************************************************************
 */

package nl.knaw.dans.easy.ingest_flow

import java.io.File
import java.net.URL

import com.yourmediashelf.fedora.client.FedoraCredentials
import nl.knaw.dans.easy._
import nl.knaw.dans.easy.archivebag.EasyArchiveBag
import nl.knaw.dans.easy.fsrdb.FsRdbUpdater
import nl.knaw.dans.easy.ingest.EasyIngest
import nl.knaw.dans.easy.ingest.EasyIngest.PidDictionary
import nl.knaw.dans.easy.solr.EasyUpdateSolrIndex
import nl.knaw.dans.easy.stage.EasyStageDataset
import org.apache.commons.configuration.PropertiesConfiguration
import org.apache.commons.io.FileUtils
import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}
import scalaj.http.Http

object EasyIngestFlow {
  val log = LoggerFactory.getLogger(getClass)


  case class Settings(storageUser: String,
                      storagePassword: String,
                      storageServiceUrl: URL,
                      fedoraCredentials: FedoraCredentials,
                      numSyncTries: Int,
                      syncDelay: Long,
                      ownerId: String,
                      bagStorageLocation: String,
                      bagitDir: File,
                      sdoSetDir: File,
                      DOI: String,
                      postgresURL: String,
                      solr: String,
                      pidgen: String)

  def main(args: Array[String]) {
    val conf = new Conf(args)
    val homeDir = new File(System.getenv("EASY_INGEST_FLOW_HOME"))
    val props = new PropertiesConfiguration(new File(homeDir, "cfg/application.properties"))
    implicit val settings = Settings(
      storageUser = props.getString("storage.user"),
      storagePassword = props.getString("storage.password"),
      storageServiceUrl = new URL(props.getString("storage.service-url")),
      fedoraCredentials = new FedoraCredentials(
        props.getString("fcrepo.url"),
        props.getString("fcrepo.user"),
        props.getString("fcrepo.password")),
      numSyncTries = props.getInt("sync.num-tries"),
      syncDelay = props.getInt("sync.delay"),
      ownerId = props.getString("easy.owner"),
      bagStorageLocation = props.getString("storage.base-url"),
      bagitDir = conf.depositDir(),
      sdoSetDir = new File(props.getString("staging.root-dir"), conf.depositDir().getName),
      DOI = "10.1000/xyz123", // TODO: get this from the deposit metadata
      postgresURL = props.getString("fsrdb.connection-url"),
      solr = props.getString("solr.update-url"),
      pidgen = props.getString("pid-generator.url"))

    val datasetPid = run().get
    log.info(s"Finished, dataset pid: $datasetPid")
  }

  def run()(implicit s: Settings): Try[String] = {
    for {
      urn <- requestURN()
      datasetDir <- archiveBag()
      _ <- stageDataset(urn, datasetDir)
      pidDictionary <- ingestDataset()
      datasetPid <- getDatasetPid(pidDictionary)
      _ <- waitForFedoraSync(datasetPid, pidDictionary, s.numSyncTries, s.syncDelay)
      _ <- updateFsRdb(datasetPid)
      _ <- updateSolr(datasetPid)
      _ <- deleteSdoSetDir()
    } yield datasetPid
  }

  def archiveBag()(implicit s: Settings): Try[String] = {
     EasyArchiveBag.run(archivebag.Settings(
       username = s.storageUser,
       password = s.storagePassword,
       bagDir = s.bagitDir,
       storageDepositService =  s.storageServiceUrl)
     )
  }

  def stageDataset(urn: String, datasetDir: String)(implicit s: Settings): Try[Unit] = {
    log.info("Staging dataset")
    EasyStageDataset.run(stage.Settings(
      ownerId = s.ownerId,
      bagStorageLocation = s.bagStorageLocation + "/" + datasetDir,
      bagitDir = s.bagitDir,
      sdoSetDir = s.sdoSetDir,
      URN = urn,
      DOI = s.DOI,
      fedoraUrl = s.fedoraCredentials.getBaseUrl,
      fedoraUser = s.fedoraCredentials.getUsername,
      fedoraPassword =  s.fedoraCredentials.getPassword))
  }

  def ingestDataset()(implicit s: Settings): Try[PidDictionary] = {
    log.info("Ingesting staged digital object into Fedora")
    EasyIngest.run(ingest.Settings(s.fedoraCredentials, s.sdoSetDir))
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

  def deleteSdoSetDir()(implicit s: Settings): Try[Unit] = Try {
    log.info(s"Removing staged dataset from ${s.sdoSetDir}")
    FileUtils.deleteDirectory(s.sdoSetDir)
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
