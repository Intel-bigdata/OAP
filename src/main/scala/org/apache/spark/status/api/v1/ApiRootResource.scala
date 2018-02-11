/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.status.api.v1

import java.util.zip.ZipOutputStream
import javax.servlet.ServletContext
import javax.ws.rs._
import javax.ws.rs.core.{Context, Response}

import org.eclipse.jetty.server.handler.ContextHandler
import org.eclipse.jetty.servlet.{ServletContextHandler, ServletHolder}
import org.glassfish.jersey.server.ServerProperties
import org.glassfish.jersey.servlet.ServletContainer

import org.apache.spark.SecurityManager
import org.apache.spark.ui.SparkUI

/**
 * Main entry point for serving spark application metrics as json, using JAX-RS.
 *
 * Each resource should have endpoints that return **public** classes defined in api.scala.  Mima
 * binary compatibility checks ensure that we don't inadvertently make changes that break the api.
 * The returned objects are automatically converted to json by jackson with JacksonMessageWriter.
 * In addition, there are a number of tests in HistoryServerSuite that compare the json to "golden
 * files".  Any changes and additions should be reflected there as well -- see the notes in
 * HistoryServerSuite.
 */
@Path("/v1")
private[v1] class ApiRootResource extends UIRootFromServletContext {

  @Path("applications")
  def getApplicationList(): ApplicationListResource = {
    new ApplicationListResource(uiRoot)
  }

  @Path("applications/{appId}")
  def getApplication(): OneApplicationResource = {
    new OneApplicationResource(uiRoot)
  }

  @Path("applications/{appId}/{attemptId}/jobs")
  def getJobs(
      @PathParam("appId") appId: String,
      @PathParam("attemptId") attemptId: String): AllJobsResource = {
    uiRoot.withSparkUI(appId, Some(attemptId)) { ui =>
      new AllJobsResource(ui)
    }
  }

  @Path("applications/{appId}/jobs")
  def getJobs(@PathParam("appId") appId: String): AllJobsResource = {
    uiRoot.withSparkUI(appId, None) { ui =>
      new AllJobsResource(ui)
    }
  }

  @Path("applications/{appId}/jobs/{jobId: \\d+}")
  def getJob(@PathParam("appId") appId: String): OneJobResource = {
    uiRoot.withSparkUI(appId, None) { ui =>
      new OneJobResource(ui)
    }
  }

  @Path("applications/{appId}/{attemptId}/jobs/{jobId: \\d+}")
  def getJob(
      @PathParam("appId") appId: String,
      @PathParam("attemptId") attemptId: String): OneJobResource = {
    uiRoot.withSparkUI(appId, Some(attemptId)) { ui =>
      new OneJobResource(ui)
    }
  }

  @Path("applications/{appId}/executors")
  def getExecutors(@PathParam("appId") appId: String): ExecutorListResource = {
    uiRoot.withSparkUI(appId, None) { ui =>
      new ExecutorListResource(ui)
    }
  }

  @Path("applications/{appId}/allexecutors")
  def getAllExecutors(@PathParam("appId") appId: String): AllExecutorListResource = {
    uiRoot.withSparkUI(appId, None) { ui =>
      new AllExecutorListResource(ui)
    }
  }

  @Path("applications/{appId}/{attemptId}/executors")
  def getExecutors(
      @PathParam("appId") appId: String,
      @PathParam("attemptId") attemptId: String): ExecutorListResource = {
    uiRoot.withSparkUI(appId, Some(attemptId)) { ui =>
      new ExecutorListResource(ui)
    }
  }

  @Path("applications/{appId}/{attemptId}/allexecutors")
  def getAllExecutors(
      @PathParam("appId") appId: String,
      @PathParam("attemptId") attemptId: String): AllExecutorListResource = {
    uiRoot.withSparkUI(appId, Some(attemptId)) { ui =>
      new AllExecutorListResource(ui)
    }
  }


  @Path("applications/{appId}/stages")
  def getStages(@PathParam("appId") appId: String): AllStagesResource = {
    uiRoot.withSparkUI(appId, None) { ui =>
      new AllStagesResource(ui)
    }
  }

  @Path("applications/{appId}/{attemptId}/stages")
  def getStages(
      @PathParam("appId") appId: String,
      @PathParam("attemptId") attemptId: String): AllStagesResource = {
    uiRoot.withSparkUI(appId, Some(attemptId)) { ui =>
      new AllStagesResource(ui)
    }
  }

  @Path("applications/{appId}/stages/{stageId: \\d+}")
  def getStage(@PathParam("appId") appId: String): OneStageResource = {
    uiRoot.withSparkUI(appId, None) { ui =>
      new OneStageResource(ui)
    }
  }

  @Path("applications/{appId}/{attemptId}/stages/{stageId: \\d+}")
  def getStage(
      @PathParam("appId") appId: String,
      @PathParam("attemptId") attemptId: String): OneStageResource = {
    uiRoot.withSparkUI(appId, Some(attemptId)) { ui =>
      new OneStageResource(ui)
    }
  }

  @Path("applications/{appId}/storage/rdd")
  def getRdds(@PathParam("appId") appId: String): AllRDDResource = {
    uiRoot.withSparkUI(appId, None) { ui =>
      new AllRDDResource(ui)
    }
  }

  @Path("applications/{appId}/{attemptId}/storage/rdd")
  def getRdds(
      @PathParam("appId") appId: String,
      @PathParam("attemptId") attemptId: String): AllRDDResource = {
    uiRoot.withSparkUI(appId, Some(attemptId)) { ui =>
      new AllRDDResource(ui)
    }
  }

  @Path("applications/{appId}/storage/rdd/{rddId: \\d+}")
  def getRdd(@PathParam("appId") appId: String): OneRDDResource = {
    uiRoot.withSparkUI(appId, None) { ui =>
      new OneRDDResource(ui)
    }
  }

  @Path("applications/{appId}/{attemptId}/storage/rdd/{rddId: \\d+}")
  def getRdd(
      @PathParam("appId") appId: String,
      @PathParam("attemptId") attemptId: String): OneRDDResource = {
    uiRoot.withSparkUI(appId, Some(attemptId)) { ui =>
      new OneRDDResource(ui)
    }
  }

  @Path("applications/{appId}/logs")
  def getEventLogs(
      @PathParam("appId") appId: String): EventLogDownloadResource = {
    new EventLogDownloadResource(uiRoot, appId, None)
  }

  @Path("applications/{appId}/{attemptId}/logs")
  def getEventLogs(
      @PathParam("appId") appId: String,
      @PathParam("attemptId") attemptId: String): EventLogDownloadResource = {
    new EventLogDownloadResource(uiRoot, appId, Some(attemptId))
  }

  @Path("version")
  def getVersion(): VersionResource = {
    new VersionResource(uiRoot)
  }
}
