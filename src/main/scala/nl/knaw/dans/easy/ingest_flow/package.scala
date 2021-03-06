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
package nl.knaw.dans.easy

import java.io.{File, IOException}
import java.net.URL
import java.util.Properties

import com.yourmediashelf.fedora.client.FedoraCredentials
import org.apache.commons.configuration.PropertiesConfiguration
import org.apache.commons.io.FileUtils

import scala.util.{Failure, Success, Try}

package object ingest_flow {

  // values
  val stateArchived = "ARCHIVED"
  val stateRejected = "REJECTED"

  // case classes
  case class Settings(storageUser: String,
                      storagePassword: String,
                      storageServiceUrl: URL,
                      fedoraCredentials: FedoraCredentials,
                      numSyncTries: Int,
                      syncDelay: Long,
                      ownerId: String,
                      datasetAccessBaseUrl: String,
                      depositDir: File,
                      checkInterval: Int,
                      maxCheckCount: Int,
                      sdoSetDir: File,
                      postgresURL: String,
                      solr: String,
                      pidgen: String)

  object Version {
    def apply(): String = {
      val props = new Properties()
      props.load(Version.getClass.getResourceAsStream("/Version.properties"))
      props.getProperty("application.version")
    }
  }

  implicit class TryExceptionHandling[T](val t: Try[T]) extends AnyVal {
    /** Terminating operator for `Try` that converts the `Failure` case in a value.
      *
      * @param handle converts `Throwable` to a value of type `T`
      * @return either the value inside `Try` (on success) or the result of `handle` (on failure)
      */
    def onError[S >: T](handle: Throwable => S): S = {
      t match {
        case Success(value) => value
        case Failure(throwable) => handle(throwable)
      }
    }
  }

  implicit class FileExtensions(val file: File) extends AnyVal {
    /**
      * Deletes a directory recursively.
      *
      * @throws IOException in case deletion is unsuccessful
      */
    def deleteDirectory() = FileUtils.deleteDirectory(file)
  }

  def depositPropertiesFile(implicit settings: Settings) = {
    new File(settings.depositDir, "deposit.properties")
  }

  def bagDir(implicit settings: Settings): Option[File] = {
    settings.depositDir.listFiles.find(f => f.isDirectory && f.getName != ".git")
  }

  def datasetMetadata(implicit settings: Settings) = {
    bagDir.map(bag => new File(bag, "metadata/dataset.xml"))
  }

  def gitDir(implicit settings: Settings) = new File(settings.depositDir, ".git")

  def isMendeley(implicit settings: Settings): Boolean = {
    settings.ownerId == "mendeleydata" || settings.ownerId == "mendeltest"
  }

  def setDepositState(state: String, description: String)(implicit s: Settings): Try[Unit] = Try {
    val stateFile = new PropertiesConfiguration(depositPropertiesFile)
    stateFile.setProperty("state.label", state)
    stateFile.setProperty("state.description", description)
    stateFile.save()
  }
}
