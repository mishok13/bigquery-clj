(ns io.screen6.bigquery.load
  "Loading data into BigQuery"
  (:require
   [cheshire.core :as json]
   [clj-http.client :as http]
   [clojure.java.io :as io]))

(defn schema
  [project dataset table columns]
  {:configuration
   {:load
    {;; :sourceFormat "<required for JSON files>",
     :schema {:fields (map (fn prepare-column [[name type]] {:name name :type type}) columns)}
     :destinationTable {:projectId project
                        :datasetId dataset
                        :tableId table}}}})

(def ^:private RESUMABLE-UPLOAD-URL
  "https://www.googleapis.com/upload/bigquery/v2/projects/%s/jobs?uploadType=resumable")

(defn multipart
  "Multipart upload

  The steps are:
  1. Get resumable session id
  2. Upload file
  3. Handle failures"
  [bq project dataset table columns path]
  ;; Just get a new token every time, that's the stupidest and yet
  ;; most effective way to handle this for now
  (.refreshToken bq)
  (let [content-length (.length (io/file path))
        session-url (get-in (http/post (format RESUMABLE-UPLOAD-URL project)
                                       {:body (json/encode (schema project dataset table columns))
                                        :headers {"X-Upload-Content-Type" "application/octet-stream"
                                                  "X-Upload-Content-Length" content-length
                                                  "Content-Type" "application/json; charset=UTF-8"
                                                  "Authorization" (format "Bearer %s" (.getAccessToken bq))}}) [:headers "Location"])]
    (http/put session-url
              {:body (slurp path)
               :headers {"Authorization" (format "Bearer %s" (.getAccessToken bq))
                         "Content-Type" "application/octet-stream"}})))

(defn jobs
  [bq project]
  (http/get (format "https://www.googleapis.com/bigquery/v2/projects/%s/jobs" project)
            {:headers {"Authorization" (format "Bearer %s" (.getAccessToken bq))}}))
