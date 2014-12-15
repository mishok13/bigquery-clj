(ns io.screen6.bigquery.load
  "Loading data into BigQuery"
  (:require
   [cheshire.core :as json]
   [clj-http.client :as http]
   [clojure.java.io :as io]
   [io.screen6.bigquery.auth :as auth]))

(def ^:private RESUMABLE-UPLOAD-URL
  "https://www.googleapis.com/upload/bigquery/v2/projects/%s/jobs?uploadType=resumable")

(defn schema
  [project dataset table columns]
  ;; There's also a :sourceFormat, but that's only for JSON files.
  ;; TODO: add support for sourceFormat
  {:configuration
   {:load
    {:schema {:fields (map (fn prepare-column [[name type]] {:name name :type type}) columns)}
     :destinationTable {:projectId project
                        :datasetId dataset
                        :tableId table}}}})

(defn multipart
  "Multipart upload

  The steps are:
  1. Get resumable session id
  2. Upload file
  3. Handle failures"
  [credential project dataset table columns path]
  ;; Just get a new token every time, that's the stupidest and yet
  ;; most effective way to handle this for now
  (auth/refresh! credential)
  (assert (.exists (io/file path)) "File doesn't exist")
  (let [content-length (.length (io/file path))
        session-url (-> (format RESUMABLE-UPLOAD-URL project)
                        (http/post
                         {:body (json/encode (schema project dataset table columns))
                          :headers {"X-Upload-Content-Type" "application/octet-stream"
                                    "X-Upload-Content-Length" content-length
                                    "Content-Type" "application/json; charset=UTF-8"
                                    "Authorization" (format "Bearer %s" (auth/access-token credential))}})
                        (get-in  [:headers "Location"]))
        job (http/put session-url
                      {:body (slurp path)
                       :headers {"Authorization" (format "Bearer %s" (auth/access-token credential))
                                 "Content-Type" "application/octet-stream"}})]
    ;; TODO: actually wait for result!
    (let [link (-> job :body (json/decode true) :selfLink)]
      (loop [status nil]
        (if (= status "DONE")
          true
          (do
            (Thread/sleep 500)
            (recur (-> link
                       (http/get {:headers {"Authorization" (format "Bearer %s" (auth/access-token credential))}})
                       (:body)
                       (json/decode true)
                       (get-in [:status :state])))))))))

(defn jobs
  [bq project]
  (http/get (format "https://www.googleapis.com/bigquery/v2/projects/%s/jobs" project)
            {:headers {"Authorization" (format "Bearer %s" (.getAccessToken bq))}}))
