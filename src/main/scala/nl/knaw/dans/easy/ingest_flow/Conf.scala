/*******************************************************************************
  * Copyright 2015 DANS - Data Archiving and Networked Services
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  *   http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  ******************************************************************************/

package nl.knaw.dans.easy.ingest_flow

import java.io.File

import org.rogach.scallop.ScallopConf

class Conf(args: Seq[String]) extends ScallopConf(args) {
  printedName = "easy-ingest-flow"
  version(s"$printedName ${Version()}")
  banner("""
                |Perform the complete flow of actions to ingest a deposit into the archive
                |
                |Usage: easy-ingest-flow <deposit-dir>
                |
                |Options:
                |""".stripMargin)
  val depositDir = trailArg[File](
    name = "<deposit-dir>",
    descr = "Deposit-directory to ingest",
    required = true)
 
} 