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

import java.io.File
import java.net.URL

import nl.knaw.dans.easy._
import nl.knaw.dans.easy.archivebag.EasyArchiveBag
import nl.knaw.dans.easy.fsrdb.FsRdbUpdater
import nl.knaw.dans.easy.ingest.EasyIngest
import nl.knaw.dans.easy.ingest.EasyIngest.PidDictionary
import nl.knaw.dans.easy.ingest_flow.{CommandLineOptions => cmd}
import nl.knaw.dans.easy.solr.EasyUpdateSolrIndex
import nl.knaw.dans.easy.stage.EasyStageDataset
import org.apache.commons.io.FileUtils
import org.eclipse.jgit.api.Git
import org.joda.time.DateTime
import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.sys.error
import scala.util.{Failure, Success, Try}
import scala.xml.{Node, Elem, XML}
import scalaj.http.Http

object EasyIngestFlow {
  val log = LoggerFactory.getLogger(getClass)
  val STATE_SUBMITTED = "SUBMITTED"

  def main(args: Array[String]) {
    log.debug("Starting application.")
    implicit val settings = cmd parse args

    run()
      .map(datasetPid => log.info(s"Finished, dataset pid: $datasetPid"))
      .onError(e => {
        setDepositStateToRejected(e.getMessage)
        tagDepositAsRejected(e.getMessage)
        log.error(e.getMessage)
      })
  }

  def run()(implicit s: Settings): Try[String] = {
    for {
      _ <- assertNoVirusesInDeposit()
      xml <- loadDDM()
      _ <- assertNoAccessSet(xml)
      urn <- requestUrn()
      (doi, otherAccessDOI) <- getDoi(xml)
      _ <- stageDataset(urn, doi, otherAccessDOI)
      pidDictionary <- ingestDataset()
      datasetPid <- getDatasetPid(pidDictionary)
      _ <- waitForFedoraSync(datasetPid, pidDictionary, s.numSyncTries, s.syncDelay)
      _ <- updateFsRdb(datasetPid)
      _ <- updateSolr(datasetPid)
      _ <- deleteSdoSetDir()
      (storageDatasetDir, state) <- archiveBag(urn)
      _ = log.info(s"Archival storage service returned: $state")
      if state == STATE_SUBMITTED
      _ <- setDepositStateToArchived(datasetPid)
      _ <- deleteBag()
      _ <- deleteGitRepo()
    } yield datasetPid
  }

  def assertNoVirusesInDeposit()(implicit s: Settings): Try[Unit] = Try {
    import scala.sys.process._
    val cmd = s"/usr/bin/clamscan -r -i ${s.depositDir}"
    log.info(s"Scanning for viruses: $cmd")
    val output = new StringBuilder
    val exit = Process(cmd)! ProcessLogger(line => output ++= s"$line\n")
    if(exit > 0) throw new RuntimeException(s"Detected a virus, clamscan output:\n${output.toString}")
    log.info("No viruses found")
  }

  def loadDDM()(implicit settings: Settings) = Try {
    getBagDir(settings.depositDir)
      .map(bag => XML.loadFile(new File(bag, "metadata/dataset.xml")))
      .getOrElse(error(s"Could not find bag in deposit: ${settings.depositDir}"))
  }

  // TODO this assertion fails when using this tool for ingesting multi-deposit (non-mendeley) datasets
  def assertNoAccessSet(xml: Elem): Try[Unit] = Try {
    (xml \\ "DDM" \ "profile" \ "accessRights").map(_.text) match {
      case Seq() => error("Dataset metadata contains no access rights")
      case Seq(ar) => if (ar != "NO_ACCESS") error("Dataset DOI is other access but accessrights are NOT set to NO_ACCESS")
      case multiple => error(s"Dataset metadata contains multiple access rights: $multiple")
    }
  }

  // TODO deze moet niet als het niet van mendeley komt
  def archiveBag(urn: String)(implicit s: Settings): Try[(String, String)] = {
    log.info("Sending bag to archival storage")
    EasyArchiveBag.run(archivebag.Settings(
       username = s.storageUser,
       password = s.storagePassword,
       checkInterval=s.checkInterval,
       maxCheckCount=s.maxCheckCount,
       bagDir = getBagDir(s.depositDir).get,
       slug = Some(urn),
       storageDepositService =  s.storageServiceUrl)
     )
  }

  def stageDataset(urn: String, doi: String, otherAccessDOI: Boolean)(implicit s: Settings): Try[Unit] = {
    log.info("Staging dataset")
    EasyStageDataset.run(stage.Settings(
      ownerId = s.ownerId,
      submissionTimestamp = getSubmissionTimestamp(s.depositDir),
      bagitDir = getBagDir(s.depositDir).get,
      sdoSetDir = s.sdoSetDir,
      URN = urn,
      DOI = doi,
      otherAccessDOI = otherAccessDOI,
      fedoraUrl = s.fedoraCredentials.getBaseUrl,
      fedoraUser = s.fedoraCredentials.getUsername,
      fedoraPassword =  s.fedoraCredentials.getPassword))
  }

  def getSubmissionTimestamp(depositDir: File): String =
    new DateTime(new File(depositDir, "deposit.properties").lastModified).toString

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

  def deleteBag()(implicit s: Settings): Try[Unit] = Try {
    val bag = getBagDir(s.depositDir).get
    log.info(s"Removing deposit data at $bag")
    FileUtils.deleteDirectory(bag)
  }

  def setDepositStateToArchived(datasetPid: String)(implicit s: Settings): Try[Unit] = Try {
    DepositState.setDepositState("ARCHIVED", s.datasetAccessBaseUrl + "/" + datasetPid)
  }

  def deleteGitRepo()(implicit s: Settings): Try[Unit] = Try {
    val gitDir = new File(s.depositDir, ".git")
    if(gitDir.exists) {
      log.info(s"Removing git repo at $gitDir ")
      FileUtils.deleteDirectory(gitDir)
    }
  }

  def tagDepositAsRejected(reason: String)(implicit s:Settings): Try[Unit] = Try {
    log.info("Tagging deposit as REJECTED")
    Git.open(s.depositDir)
      .tag()
      .setName("state=REJECTED")
      .setMessage(reason).call()
  }

  def setDepositStateToRejected(reason: String)(implicit s: Settings): Try[Unit] = Try {
    log.info("Setting deposit state to REJECTED")
    DepositState.setDepositState("REJECTED", reason)
  }

  def getDatasetPid(pidDictionary: PidDictionary): Try[String] = {
    pidDictionary.values
      .find(_ startsWith "easy-dataset")
      .map(Success(_))
      .getOrElse(Failure(new RuntimeException("SDO-set didn't contain a dataset object.")))
  }

  def requestUrn()(implicit s: Settings): Try[String] = requestPid("urn")

  def requestDoi()(implicit s: Settings): Try[String] = requestPid("doi")

  // TODO this should be an Observable: does network requests
  // use https://github.com/ReactiveX/RxApacheHttp
  def requestPid(pidType: String)(implicit s: Settings): Try[String] =
    Try {
      Http(s"${s.pidgen}?type=$pidType" )
        .timeout(connTimeoutMs = 10000, readTimeoutMs = 50000)
        .postForm.asString
    }.flatMap(r =>
      if (r.code == 200) {
        val pid = r.body
        log.info(s"Requested ${pidType.toUpperCase}: $pid")
        Success(pid)
      } else Failure(new RuntimeException(s"PID Generator failed: ${r.body}")))

  // TODO hardcoded ownerId's???
  // TODO tuple -> case class or type aliasses: more descriptive code
  //      what does the Boolean or String mean? You can't derive that from the type definition
  def getDoi(xml: Elem)(implicit s: Settings): Try[(String, Boolean)] = Try {
    if (s.ownerId == "mendeleydata" || s.ownerId == "mendeltest") (getDoiFromDdm(xml).get, true)
    else (requestDoi().get, false)
  }

  def getDoiFromDdm(xml: Elem): Try[String] = {
    val ids = xml \\ "DDM" \ "dcmiMetadata" \ "identifier"
    val dois = ids.filter(hasXsiType(_, "http://easy.dans.knaw.nl/schemas/vocab/identifier-type/", "DOI"))
    if(dois.size == 1) Try(dois(0).text)
    else if(dois.isEmpty) Failure(new RuntimeException("Dataset metadata doesn't contain a DOI"))
    else Failure(new RuntimeException(s"Dataset metadata contains more than one DOI: $dois"))
  }

  def hasXsiType(e: Node, attributeNamespace: String, attributeValue: String): Boolean = {
    e.head
      .attribute("http://www.w3.org/2001/XMLSchema-instance", "type")
      .exists {
        case Seq(n) =>
          val Array(pref, label) = n.text.split("\\:")
          e.head.getNamespace(pref) == attributeNamespace && label == attributeValue
        case _ => false
      }
  }

  def waitForFedoraSync(datasetPid: String, pidDictionary: PidDictionary, numTries: Int, delayMillis: Long)(implicit s: Settings): Try[Unit] = {
    val expectedPids = pidDictionary.values.toSet - datasetPid
    @tailrec def loop(n: Int): Try[Unit] = {
      log.info(s"Check whether Fedora is synced. Tries left: $n.")
      if (n <= 0)
        Failure(new RuntimeException(s"Fedora didn't sync in time. Dataset: $datasetPid. Expected pids: $expectedPids"))
      else
        queryPids(datasetPid).map(pids => pids.size == expectedPids.size && pids.toSet == expectedPids) match {
          case Success(true) => Success(Unit)
          case _ => loop(Try { Thread.sleep(delayMillis) } map (_ => n - 1) getOrElse n)
        }
    }
    loop(numTries)
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
}
