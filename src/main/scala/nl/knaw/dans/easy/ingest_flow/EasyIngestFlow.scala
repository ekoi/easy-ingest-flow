/**
  * Copyright (C) 2015-2016 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package nl.knaw.dans.easy.ingest_flow

import nl.knaw.dans.easy.ingest_flow.{CommandLineOptions => cmd}
import org.eclipse.jgit.api.Git
import org.slf4j.LoggerFactory

import scala.util.Try

object EasyIngestFlow {
  val log = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]) {
    log.debug("Starting application.")
    implicit val settings = cmd parse args

    execute.flatMap(_.run)
      .map(datasetPid => log.info(s"Finished, dataset pid: $datasetPid"))
      .onError(e => {
        setDepositStateToRejected(e.getMessage)
        // TODO this call is harmfull, as the deposits are not git repositories anymore!
//        tagDepositAsRejected(e.getMessage)
        log.error(e.getMessage)
      })
  }

  def execute(implicit settings: Settings): Try[Execution] = {
    assertNoVirusesInDeposit
      .map(_ => if (isMendeley) MendeleyExecution else MultiDepositExecution)
  }

  def assertNoVirusesInDeposit(implicit s: Settings): Try[Unit] = Try {
    import scala.sys.process._
    val cmd = s"/usr/bin/clamscan -r -i ${s.depositDir}"
    log.info(s"Scanning for viruses: $cmd")
    val output = new StringBuilder
    val exit = Process(cmd) ! ProcessLogger(line => output ++= s"$line\n")
    if (exit > 0) throw new RuntimeException(s"Detected a virus, clamscan output:\n${output.toString}")
    log.info("No viruses found")
  }

  def setDepositStateToRejected(reason: String)(implicit s: Settings): Try[Unit] = Try {
    log.info("Setting deposit state to REJECTED")
    setDepositState("REJECTED", reason)
  }

  def tagDepositAsRejected(reason: String)(implicit s: Settings): Try[Unit] = Try {
    log.info("Tagging deposit as REJECTED")
    Git.open(s.depositDir)
      .tag()
      .setName("state=REJECTED")
      .setMessage(reason).call()
  }
}
