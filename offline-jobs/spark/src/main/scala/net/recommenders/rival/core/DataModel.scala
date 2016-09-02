/*
 * Copyright 2015 recommenders.net.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.recommenders.rival.core

import java.io.Serializable
import java.util.HashMap
import java.util.HashSet
import java._
import java.util.Set

/*object DataModel (userItemPreference: util.Map[U, util.Map[I, Double]], itemUserPreference: util.Map[I, util.Map[U, Double]], userItemTimestamp: util.Map[U, util.Map[I, util.Set[Long]]] ) {
  this.userItemPreferences = userItemPreference
  this.itemUserPreferences = itemUserPreference
  this.userItemTimestamps = userItemTimestamp
}*/

case class DataModel[U, I](userItemPreferences : util.Map[U, util.Map[I, Double]], itemUserPreferences: util.Map[I, util.Map[U, Double]], userItemTimestamps: util.Map[U, util.Map[I, util.Set[Long]]]) {
  def this() {
    this(new util.HashMap[U, util.Map[I, Double]], new util.HashMap[I, util.Map[U, Double]], new util.HashMap[U, util.Map[I, util.Set[Long]]])
  }

  def getItemUserPreferences: util.Map[I, util.Map[U, Double]] = {
    return itemUserPreferences
  }

  def getUserItemPreferences: util.Map[U, util.Map[I, Double]] = {
    return userItemPreferences
  }

  def getUserItemTimestamps: util.Map[U, util.Map[I, util.Set[Long]]] = {
    return userItemTimestamps
  }

  def addPreference(u: U, i: I, d: Double) {
    var userPreferences: util.Map[I, Double] = userItemPreferences.get(u)
    if (userPreferences == null) {
      userPreferences = new util.HashMap[I, Double]
      userItemPreferences.put(u, userPreferences)
    }
    var preference: Double = userPreferences.get(i)
    if (preference == null) {
      preference = 0.0
    }
    preference += d
    userPreferences.put(i, preference)
    var itemPreferences: util.Map[U, Double] = itemUserPreferences.get(i)
    if (itemPreferences == null) {
      itemPreferences = new util.HashMap[U, Double]
      itemUserPreferences.put(i, itemPreferences)
    }
    itemPreferences.put(u, preference)
  }

  def addTimestamp(u: U, i: I, t: Long) {
    var userTimestamps: util.Map[I, util.Set[Long]] = userItemTimestamps.get(u)
    if (userTimestamps == null) {
      userTimestamps = new util.HashMap[I, util.Set[Long]]
      userItemTimestamps.put(u, userTimestamps)
    }
    var timestamps: util.Set[Long] = userTimestamps.get(i)
    if (timestamps == null) {
      timestamps = new util.HashSet[Long]
      userTimestamps.put(i, timestamps)
    }
    timestamps.add(t)
  }

  def getItems: util.Set[I] = {
    return getItemUserPreferences.keySet
  }

  def getUsers: util.Set[U] = {
    return getUserItemPreferences.keySet
  }

  def getNumItems: Int = {
    return getItems.size
  }

  def getNumUsers: Int = {
    return getUsers.size
  }

  def clear {
    userItemPreferences.clear
    userItemTimestamps.clear
    itemUserPreferences.clear
  }
}