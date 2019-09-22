(ns lakepend.db
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.string :as str]))

(def db-conf {:classname "org.sqlite.JDBC"
              :subprotocol "sqlite"
              :subname "resources/database.db"})

(defn insert-weather-data
  [weather-data]
  (jdbc/insert-multi! db-conf :weather weather-data))

(defn find-last-datetime
  []
  (-> (jdbc/query db-conf "select recorded_at from weather order by recorded_at desc limit 1")
      first
      :recorded_at))

(defn avg-median-query)
  "select avg(wind_speed) as avg_wind_speed,
  avg(air_temp) as avg_air_tmp,



