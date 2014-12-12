(ns io.screen6.bigquery.load
  "Loading data into BigQuery"
  (:require
   [clj-http.client :as http]
   [cheshire.core :as json]))

(defn schema
  [project dataset table columns]
  {:configuration
   {:load
    {;; :sourceFormat "<required for JSON files>",
     :schema {:fields (map (fn prepare-column [[name type]] {:name name :type type}) columns)}
     :destinationTable {:projectId project
                        :datasetId dataset
                        :tableId table}}}})

(defn multipart
  "Multipart upload"
  [bq project-id path]
  (.refreshToken bq)
  (let [token (.getAccessToken bq)
        url (format "https://www.googleapis.com/upload/bigquery/v2/projects/%s/jobs?uploadType=resumable" project-id)]
    (http/post url
               {:throw-entire-message? true
                :debug true
                :debug-body true
                :body (json/encode (schema "dedup-bong-22" "uploadtest" "test" [["foo" "STRING"]]))
                :headers {"X-Upload-Content-Type" "application/octet-stream"
                          "X-Upload-Content-Length" "2000000"
                          "Content-Type" "application/json; charset=UTF-8"
                          ;; "Content-Length" "0"
                          "Authorization" (format "Bearer %s" token)}})))
