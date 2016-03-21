package nl.knaw.dans.easy.ingest_flow

import java.io.File

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
      _ <- stageDataset(urn, doi, true) // TODO refactor?
      pidDictionary <- ingestDataset()
      datasetPid <- getDatasetPid(pidDictionary)
      _ <- waitForFedoraSync(datasetPid, pidDictionary)
      _ <- updateFsRdb(datasetPid)
      _ <- updateSolr(datasetPid)
      _ <- deleteSdoSetDir()
      (storageDatasetDir, state) <- archiveBag(urn) // TODO refactor? what to do with storageDatasetDir and state? only logging?
      _ = log.info(s"Archival storage service returned: $state")
      if state == STATE_SUBMITTED
      _ <- setDepositStateToArchived(datasetPid)
      _ <- deleteBag()
      _ <- deleteGitRepo()
    } yield datasetPid
  }

  def loadDDM()(implicit settings: Settings) = Try {
    getBagDir(settings.depositDir)
      .map(bag => XML.loadFile(new File(bag, "metadata/dataset.xml")))
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
      bagDir = getBagDir(s.depositDir).get,
      slug = Some(urn),
      storageDepositService = s.storageServiceUrl)
    )
  }
}
