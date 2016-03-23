package nl.knaw.dans.easy.ingest_flow

import scala.util.Try

object MultiDepositExecution extends Execution {

  def run(implicit settings: Settings): Try[String] = {
    for {
      urn <- requestUrn()
      doi <- requestDoi()
      _ <- stageDataset(urn, doi, otherAccessDOI = false)
      pidDictionary <- ingestDataset()
      datasetPid <- getDatasetPid(pidDictionary)
      _ <- waitForFedoraSync(datasetPid, pidDictionary)
      _ <- updateFsRdb(datasetPid)
      _ <- updateSolr(datasetPid)
      _ <- deleteSdoSetDir()
      _ <- setDepositStateToArchived(datasetPid)
      _ <- deleteBag()
      _ <- deleteGitRepo()
    } yield datasetPid
  }

  def requestDoi()(implicit s: Settings): Try[String] = requestPid("doi")
}
