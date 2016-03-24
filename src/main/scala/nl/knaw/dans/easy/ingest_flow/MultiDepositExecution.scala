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
