(ns lakepend.request
  (:require [clojure.java.io :as io]
            [clj-http.client :as http]))

(defn build-url
  [year]
  (str "https://raw.githubusercontent.com/lyndadotcom/LPO_weatherdata/master/Environmental_Data_Deep_Moor_" year ".txt"))

(build-url "2012")

(defn fetch-for-year
  [year]
  (let [url (build-url year)]
    (try
      (http/get url {:insecure true :as :stream})
      (catch Exception _ nil))))

(fetch-for-year "2012")

(defn fetch-seq-for-year
  [year]
  (some->> (fetch-for-year year)
           :body
           io/reader
           line-seq))

;;(fetch-seq-for-year "2012")


;;(some->> (http/get "https://github.com/ThinhPTran/LPO_weatherdata")
;;         :body))
