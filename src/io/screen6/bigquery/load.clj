(ns io.screen6.bigquery.load
  "Loading data into BigQuery"
  (:require
   [cheshire.core :as json]
   [clj-http.client :as http]
   [clojure.java.io :as io]
   [io.screen6.bigquery.auth :as auth]))

(def ^:private API-URL
  "https://www.googleapis.com")
(def ^:private RESUMABLE-UPLOAD-URL
  (format "%s/upload/bigquery/v2/projects/%%s/jobs?uploadType=resumable" API-URL))
(def ^:private MULTIPART-UPLOAD-URL
  (format "%s/upload/bigquery/v2/projects/%%s/jobs?uploadType=multipart" API-URL))

(defn- make-schema
  [{:keys [project dataset table columns]}]
  ;; There's also a :sourceFormat, but that's only for JSON files.
  ;; TODO: add support for sourceFormat
  {:configuration
   {:load
    {:schema {:fields (map (fn prepare-column [[name type]] {:name name :type type}) columns)}
     :destinationTable {:projectId project
                        :datasetId dataset
                        :tableId table}}}})

(defn resumable
  "Resumable upload

  The steps are:
  1. Get resumable session id
  2. Upload file
  3. Handle failures"
  [creds path schema & {:keys [type]}]
  (assert (= type :csv))
  ;; Schema should contain columns, table, dataset and project info
  ;; XXX: look into combining that with Google API credentials?

  ;; Just get a new token every time, that's the stupidest and yet
  ;; most effective way to handle this for now
  ;; Maybe detect type of file here?
  (auth/refresh! creds)
  (assert (.exists (io/file path)) "File doesn't exist")
  (let [content-length (.length (io/file path))
        session-url (-> (format RESUMABLE-UPLOAD-URL (:project schema))
                        (http/post
                         {:body (json/encode (make-schema schema))
                          :headers {"X-Upload-Content-Type" "application/octet-stream"
                                    "X-Upload-Content-Length" content-length
                                    "Content-Type" "application/json; charset=UTF-8"
                                    "Authorization" (format "Bearer %s" (auth/access-token creds))}})
                        (get-in  [:headers "Location"]))
        job (http/put session-url
                      {:body (slurp path)
                       :headers {"Authorization" (format "Bearer %s" (auth/access-token creds))
                                 "Content-Type" "application/octet-stream"}})]
    ;; TODO: actually wait for result!
    (let [link (-> job :body (json/decode true) :selfLink)]
      (loop [status nil]
        (if (= status "DONE")
          true
          (do
            (Thread/sleep 500)
            (recur (-> link
                       (http/get {:headers {"Authorization" (format "Bearer %s" (auth/access-token creds))}})
                       (:body)
                       (json/decode true)
                       (get-in [:status :state])))))))))

(defn- jobs
  [bq project]
  (http/get (format "https://www.googleapis.com/bigquery/v2/projects/%s/jobs" project)
            {:headers {"Authorization" (format "Bearer %s" (.getAccessToken bq))}}))
