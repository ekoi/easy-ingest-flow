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

import com.yourmediashelf.fedora.client.FedoraCredentials
import nl.knaw.dans.easy.ingest_flow.CommandLineOptions._
import nl.knaw.dans.easy.stage.lib.Fedora
import org.apache.commons.configuration.PropertiesConfiguration
import org.rogach.scallop._
import org.slf4j.LoggerFactory

object CommandLineOptions {

  val log = LoggerFactory.getLogger(getClass)

  def parse(args: Array[String]): Settings = {
    log.debug("Loading application properties ...")
    val props = {
      val homeDir = new File(System.getProperty("app.home"))
      new PropertiesConfiguration(new File(homeDir, "cfg/application.properties"))
    }
    log.debug("Parsing command line ...")
    val opts = new ScallopCommandLine(args)

    Fedora.setFedoraConnectionSettings(props.getString("fcrepo.url"), props.getString("fcrepo.user"), props.getString("fcrepo.password"))

    def getUserId(depositDir: File): String = {
      new PropertiesConfiguration(new File(depositDir, "deposit.properties")).getString("depositor.userId")
    }

    val settings = Settings(
      storageUser = props.getString("storage.user"),
      storagePassword = props.getString("storage.password"),
      storageServiceUrl = new URL(props.getString("storage.service-url")),
      fedoraCredentials = new FedoraCredentials(
        props.getString("fcrepo.url"),
        props.getString("fcrepo.user"),
        props.getString("fcrepo.password")),
      numSyncTries = props.getInt("sync.num-tries"),
      syncDelay = props.getInt("sync.delay"),
      ownerId = getUserId(opts.depositDir()),
      datasetAccessBaseUrl = props.getString("easy.dataset-access-base-url"),
      depositDir = opts.depositDir(),
      checkInterval = props.getInt("check.interval"),
      maxCheckCount = props.getInt("max.check.count"),
      sdoSetDir = new File(props.getString("staging.root-dir"), opts.depositDir().getName),
      postgresURL = props.getString("fsrdb.connection-url"),
      solr = props.getString("solr.update-url"),
      pidgen = props.getString("pid-generator.url")
    )

    log.debug(s"Using the following settings: $settings")

    settings
  }
}

class ScallopCommandLine(args: Seq[String]) extends ScallopConf(args) {

  val fileShouldExist = singleArgConverter(filename => {
    val file = new File(filename)
    if (!file.exists) {
      log.error(s"The directory '$filename' does not exist")
      throw new IllegalArgumentException(s"The directory '$filename' does not exist")
    }
    if (!file.isDirectory) {
      log.error(s"'$filename' is not a directory")
      throw new IllegalArgumentException(s"'$filename' is not a directory")
    }
    else file
  })

  printedName = "easy-ingest-flow"
  version(s"$printedName ${Version()}")
  banner("""Perform the complete flow of actions to ingest a deposit into the archive
          |
          |Usage: easy-ingest-flow <deposit-dir>
          |
          |Options:
          |""".stripMargin)

  lazy val depositDir = trailArg[File](name = "deposit-dir", required = true,
    descr = "Deposit-directory to ingest")(fileShouldExist)
  validate(depositDir) { dir =>
    if(dir.isDirectory && dir.listFiles.count(f => f.isDirectory && f.getName != ".git") == 1)
      Right(Unit)
    else
      Left(s"Invalid deposit directory $dir. It does not contain exactly one directory (other than .git).")
 }
}
