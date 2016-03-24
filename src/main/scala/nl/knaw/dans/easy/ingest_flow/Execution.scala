/**
 * Copyright (C) 2015-2016 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nl.knaw.dans.easy.ingest_flow

import java.net.URL

import nl.knaw.dans.easy.fsrdb.FsRdbUpdater
import nl.knaw.dans.easy.ingest.EasyIngest
import nl.knaw.dans.easy.ingest.EasyIngest._
import nl.knaw.dans.easy.solr.EasyUpdateSolrIndex
import nl.knaw.dans.easy.stage.EasyStageDataset
import nl.knaw.dans.easy.{fsrdb, ingest, solr, stage}
import org.joda.time.DateTime
import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}
import scalaj.http.Http

trait Execution {
  val log = LoggerFactory.getLogger(getClass)

  def run(implicit settings: Settings): Try[String]

  def requestUrn()(implicit s: Settings): Try[String] = requestPid("urn")

  // TODO this should be an Observable: does network requests
  // use https://github.com/ReactiveX/RxApacheHttp
  def requestPid(pidType: String)(implicit s: Settings): Try[String] = {
    Try {
      Http(s"${s.pidgen}?type=$pidType")
        .timeout(connTimeoutMs = 10000, readTimeoutMs = 50000)
        .postForm.asString
    }.flatMap(r =>
      if (r.code == 200) {
        val pid = r.body
        log.info(s"Requested ${pidType.toUpperCase}: $pid")
        Success(pid)
      } else Failure(new RuntimeException(s"PID Generator failed: ${r.body}")))
  }

  // TODO refactoring in the staging module
  def stageDataset(urn: String, doi: String, otherAccessDOI: Boolean)(implicit s: Settings): Try[Unit] = {
    log.info("Staging dataset")

    def getSubmissionTimestamp: String = {
      new DateTime(depositPropertiesFile.lastModified).toString
    }

    EasyStageDataset.run(stage.Settings(
      ownerId = s.ownerId,
      submissionTimestamp = getSubmissionTimestamp,
      bagitDir = bagDir.get,
      sdoSetDir = s.sdoSetDir,
      URN = urn,
      DOI = doi,
      otherAccessDOI = otherAccessDOI,
      isMendeley = isMendeley,
      fedoraUrl = s.fedoraCredentials.getBaseUrl,
      fedoraUser = s.fedoraCredentials.getUsername,
      fedoraPassword = s.fedoraCredentials.getPassword))
  }

  def ingestDataset()(implicit s: Settings): Try[PidDictionary] = {
    log.info("Ingesting staged digital object into Fedora")
    EasyIngest.run(ingest.Settings(
      fedoraCredentials = s.fedoraCredentials,
      sdo = s.sdoSetDir))
  }

  def getDatasetPid(pidDictionary: PidDictionary): Try[String] = {
    pidDictionary.values
      .find(_ startsWith "easy-dataset")
      .map(Success(_))
      .getOrElse(Failure(new RuntimeException("SDO-set didn't contain a dataset object.")))
  }

  def waitForFedoraSync(datasetPid: String, pidDictionary: PidDictionary)(implicit settings: Settings): Try[Unit] = {
    val expectedPids = pidDictionary.values.toSet - datasetPid
    @tailrec def loop(n: Int): Try[Unit] = {
      log.info(s"Check whether Fedora is synced. Tries left: $n.")
      if (n <= 0)
        Failure(new RuntimeException(s"Fedora didn't sync in time. Dataset: $datasetPid. Expected pids: $expectedPids"))
      else
        queryPids(datasetPid).map(pids => pids.size == expectedPids.size && pids.toSet == expectedPids) match {
          case Success(true) => Success(Unit)
          case _ => loop(Try { Thread.sleep(settings.syncDelay) } map (_ => n - 1) getOrElse n)
        }
    }
    loop(settings.numSyncTries)
  }

  // TODO should return an Observable: does HTTP stuff
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
    s.sdoSetDir.deleteDirectory()
  }

  def setDepositStateToArchived(datasetPid: String)(implicit s: Settings): Try[Unit] = Try {
    setDepositState(stateArchived, s.datasetAccessBaseUrl + "/" + datasetPid)
  }

  def deleteBag()(implicit s: Settings): Try[Unit] = Try {
    bagDir.foreach(bag => {
      log.info(s"Removing deposit data at $bag")
      bag.deleteDirectory()
    })
  }

  def deleteGitRepo()(implicit s: Settings): Try[Unit] = Try {
    val git = gitDir(s)
    if (git.exists) {
      log.info(s"Removing git repo at $git ")
      git.deleteDirectory()
    }
  }
}
