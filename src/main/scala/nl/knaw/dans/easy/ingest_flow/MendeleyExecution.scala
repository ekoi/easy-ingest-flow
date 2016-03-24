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

import nl.knaw.dans.easy.archivebag
import nl.knaw.dans.easy.archivebag.EasyArchiveBag

import scala.sys._
import scala.util.Try
import scala.xml.{Elem, Node, XML}

object MendeleyExecution extends Execution {

  val STATE_SUBMITTED = "SUBMITTED"

  def run(implicit settings: Settings): Try[String] = {
    for {
      xml <- loadDDM()
      _ <- assertNoAccessSet(xml)
      urn <- requestUrn()
      doi <- getDoiFromDdm(xml)
      _ <- stageDataset(urn, doi, otherAccessDOI = true)
      pidDictionary <- ingestDataset()
      datasetPid <- getDatasetPid(pidDictionary)
      _ <- waitForFedoraSync(datasetPid, pidDictionary)
      _ <- updateFsRdb(datasetPid)
      _ <- updateSolr(datasetPid)
      _ <- deleteSdoSetDir()
      (_, state) <- archiveBag(urn)
      _ = log.info(s"Archival storage service returned: $state")
      if state == STATE_SUBMITTED
      _ <- setDepositStateToArchived(datasetPid)
      _ <- deleteBag()
      _ <- deleteGitRepo()
    } yield datasetPid
  }

  def loadDDM()(implicit settings: Settings) = Try {
    datasetMetadata.map(XML.loadFile)
      .getOrElse(error(s"Could not find bag in deposit: ${settings.depositDir}"))
  }

  def assertNoAccessSet(xml: Elem): Try[Unit] = Try {
    (xml \\ "DDM" \ "profile" \ "accessRights").map(_.text) match {
      case Seq() => error("Dataset metadata contains no access rights")
      case Seq(ar) => if (ar != "NO_ACCESS") error("Dataset DOI is other access but accessrights are NOT set to NO_ACCESS")
      case multiple => error(s"Dataset metadata contains multiple access rights: $multiple")
    }
  }

  def getDoiFromDdm(xml: Elem): Try[String] = Try {
    (xml \\ "DDM" \ "dcmiMetadata" \ "identifier").filter(hasXsiType) match {
      case Seq() => error("Dataset metadata doesn't contain a DOI")
      case Seq(doi) => doi.text
      case multiple => error(s"Dataset metadata contains more than one DOI: $multiple")
    }
  }

  def hasXsiType(e: Node): Boolean = {
    e.head
      .attribute("http://www.w3.org/2001/XMLSchema-instance", "type")
      .exists {
        case Seq(n) =>
          val Array(pref, label) = n.text.split("\\:")
          val attributeNamespace = "http://easy.dans.knaw.nl/schemas/vocab/identifier-type/"
          e.head.getNamespace(pref) == attributeNamespace && label == "DOI"
        case _ => false
      }
  }

  def archiveBag(urn: String)(implicit s: Settings): Try[(String, String)] = {
    log.info("Sending bag to archival storage")
    EasyArchiveBag.run(archivebag.Settings(
      username = s.storageUser,
      password = s.storagePassword,
      checkInterval = s.checkInterval,
      maxCheckCount = s.maxCheckCount,
      bagDir = bagDir.get,
      slug = Some(urn),
      storageDepositService = s.storageServiceUrl)
    )
  }
}
