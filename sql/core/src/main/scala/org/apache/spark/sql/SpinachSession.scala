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

package org.apache.spark.sql

import java.util.concurrent.atomic.AtomicReference

import scala.reflect.ClassTag
import scala.util.control.NonFatal

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.internal.config._
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.sql.execution.ui.SQLListener
import org.apache.spark.sql.internal.SessionState
import org.apache.spark.util.Utils


class SpinachSession(sparkContext: SparkContext) extends SparkSession(sparkContext) { self =>
  @transient
  override private[sql] lazy val sessionState: SessionState = {
    // Spinach Session State is not in dependency, but in classpath, only use reflection.
    SpinachSession.reflect[SessionState, SpinachSession](
      SpinachSession.HIVE_SESSION_STATE_CLASS_NAME,
      self)
  }
}

object SpinachSession {
  class SpinachSessionBuilder extends SparkSession.Builder {
    private[this] val options = new scala.collection.mutable.HashMap[String, String]

    private[this] var userSuppliedContext: Option[SparkContext] = None

    override private[spark] def sparkContext(sparkContext: SparkContext): SpinachSessionBuilder =
      synchronized {
        userSuppliedContext = Option(sparkContext)
        this
      }

    /**
     * Sets a name for the application, which will be shown in the Spark web UI.
     * If no application name is set, a randomly generated name will be used.
     *
     * @since 2.0.0
     */
    override def appName(name: String): SpinachSessionBuilder = config("spark.app.name", name)

    /**
     * Sets a config option. Options set using this method are automatically propagated to
     * both [[SparkConf]] and SparkSession's own configuration.
     *
     * @since 2.0.0
     */
    override def config(key: String, value: String): SpinachSessionBuilder = synchronized {
      options += key -> value
      this
    }

    /**
     * Sets a config option. Options set using this method are automatically propagated to
     * both [[SparkConf]] and SparkSession's own configuration.
     *
     * @since 2.0.0
     */
    override def config(key: String, value: Long): SpinachSessionBuilder = synchronized {
      options += key -> value.toString
      this
    }

    /**
     * Sets a config option. Options set using this method are automatically propagated to
     * both [[SparkConf]] and SparkSession's own configuration.
     *
     * @since 2.0.0
     */
    override def config(key: String, value: Double): SpinachSessionBuilder = synchronized {
      options += key -> value.toString
      this
    }

    /**
     * Sets a config option. Options set using this method are automatically propagated to
     * both [[SparkConf]] and SparkSession's own configuration.
     *
     * @since 2.0.0
     */
    override def config(key: String, value: Boolean): SpinachSessionBuilder = synchronized {
      options += key -> value.toString
      this
    }

    /**
     * Sets a list of config options based on the given [[SparkConf]].
     *
     * @since 2.0.0
     */
    override def config(conf: SparkConf): SpinachSessionBuilder = synchronized {
      conf.getAll.foreach { case (k, v) => options += k -> v }
      this
    }

    /**
     * Sets the Spark master URL to connect to, such as "local" to run locally, "local[4]" to
     * run locally with 4 cores, or "spark://master:7077" to run on a Spark standalone cluster.
     *
     * @since 2.0.0
     */
    override def master(master: String): SpinachSessionBuilder = config("spark.master", master)

    /**
     * Enables Hive support, including connectivity to a persistent Hive metastore, support for
     * Hive serdes, and Hive user-defined functions.
     *
     * @since 2.0.0
     */
    override def enableHiveSupport(): SpinachSessionBuilder = synchronized {
      if (hiveClassesArePresent) {
        config(CATALOG_IMPLEMENTATION.key, "hive")
      } else {
        throw new IllegalArgumentException(
          "Unable to instantiate SparkSession with Hive support because " +
            "Hive classes are not found.")
      }
    }

    /**
     * Gets an existing [[SparkSession]] or, if there is no existing one, creates a new
     * one based on the options set in this builder.
     *
     * This method first checks whether there is a valid thread-local SparkSession,
     * and if yes, return that one. It then checks whether there is a valid global
     * default SparkSession, and if yes, return that one. If no valid global default
     * SparkSession exists, the method creates a new SparkSession and assigns the
     * newly created SparkSession as the global default.
     *
     * In case an existing SparkSession is returned, the config options specified in
     * this builder will be applied to the existing SparkSession.
     *
     * @since 2.0.0
     */
    override def getOrCreate(): SpinachSession = synchronized {
      // Get the session from current thread's active session.
      var session = activeThreadSession.get()
      if ((session ne null) && !session.sparkContext.isStopped) {
        options.foreach { case (k, v) => session.conf.set(k, v) }
        if (options.nonEmpty) {
          logWarning("Use an existing SparkSession, some configuration may not take effect.")
        }
        return session
      }

      // Global synchronization so we will only set the default session once.
      SpinachSession.synchronized {
        // If the current thread does not have an active session, get it from the global session.
        session = defaultSession.get()
        if ((session ne null) && !session.sparkContext.isStopped) {
          options.foreach { case (k, v) => session.conf.set(k, v) }
          if (options.nonEmpty) {
            logWarning("Use an existing SparkSession, some configuration may not take effect.")
          }
          return session
        }

        // No active nor global default session. Create a new one.
        val sparkContext = userSuppliedContext.getOrElse {
          // set app name if not given
          if (!options.contains("spark.app.name")) {
            options += "spark.app.name" -> java.util.UUID.randomUUID().toString
          }

          val sparkConf = new SparkConf()
          options.foreach { case (k, v) => sparkConf.set(k, v) }
          val sc = SparkContext.getOrCreate(sparkConf)
          // maybe this is an existing SparkContext, update its SparkConf which maybe used
          // by SparkSession
          options.foreach { case (k, v) => sc.conf.set(k, v) }
          sc
        }
        session = new SpinachSession(sparkContext)
        options.foreach { case (k, v) => session.conf.set(k, v) }
        defaultSession.set(session)

        // Register a successfully instantiated context to the singleton. This should be at the
        // end of the class definition so that the singleton is updated only if there is no
        // exception in the construction of the instance.
        sparkContext.addSparkListener(new SparkListener {
          override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
            defaultSession.set(null)
            sqlListener.set(null)
          }
        })
      }

      return session
    }
  }
  def builder(): SparkSession.Builder = new SpinachSessionBuilder

  private[sql] val sqlListener = new AtomicReference[SQLListener]()
  private val activeThreadSession = new InheritableThreadLocal[SpinachSession]
  private val defaultSession = new AtomicReference[SpinachSession]

  private val HIVE_SHARED_STATE_CLASS_NAME = "org.apache.spark.sql.hive.HiveSharedState"
  private val HIVE_SESSION_STATE_CLASS_NAME = "org.apache.spark.sql.hive.SpinachSessionState"

  private def reflect[T, Arg <: AnyRef](
      className: String,
      ctorArg: Arg)(implicit ctorArgTag: ClassTag[Arg]): T = {
    try {
      val clazz = Utils.classForName(className)
      val ctor = clazz.getDeclaredConstructor(ctorArgTag.runtimeClass)
      ctor.newInstance(ctorArg).asInstanceOf[T]
    } catch {
      case NonFatal(e) =>
        throw new IllegalArgumentException(s"Error while instantiating '$className':", e)
    }
  }

  private[spark] def hiveClassesArePresent: Boolean = {
    try {
      Utils.classForName(HIVE_SESSION_STATE_CLASS_NAME)
      Utils.classForName(HIVE_SHARED_STATE_CLASS_NAME)
      Utils.classForName("org.apache.hadoop.hive.conf.HiveConf")
      true
    } catch {
      case _: ClassNotFoundException | _: NoClassDefFoundError => false
    }
  }
}
