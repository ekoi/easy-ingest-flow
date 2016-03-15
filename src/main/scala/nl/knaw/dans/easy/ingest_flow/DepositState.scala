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

import org.apache.commons.configuration.PropertiesConfiguration

import scala.util.Try

object DepositState {
  // TODO no usage for this case class
  case class State(state: String, description: String, timeStamp: String)

  // TODO move to package object?
  def setDepositState(state: String, description: String)(implicit s: Settings): Try[Unit] = Try {
    val stateFile = new PropertiesConfiguration(new File(s.depositDir, "deposit.properties"))
    stateFile.setProperty("state.label", state)
    stateFile.setProperty("state.description", description)
    stateFile.save()
  }
}
