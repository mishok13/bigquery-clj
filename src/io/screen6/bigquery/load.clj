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

(defn- ^:testable make-schema
  [{:keys [columns]}]
  {:fields (map (fn prepare-column [[name type]] {:name name :type type}) columns)})

(defn- ^:testable make-table
  [{:keys [project dataset table]}]
  {:projectId project
   :datasetId dataset
   :tableId table})

(defn- ^:testable configure
  [schema
   & {:keys [delimiter] :or {delimiter ","}}]
  ;; There's also a :sourceFormat, but that's only for JSON files.
  ;; TODO: add support for sourceFormat
  ;;
  ;; Ensure we pass only 1-character long strings here, since BigQuery
  ;; API will otherwise just use the first character: "BigQuery
  ;; ... uses the first byte of the encoded string to split the data".
  (assert (= 1 (count delimiter)))
  (-> {}
      (assoc-in [:configuration :load :fieldDelimiter] delimiter)
      (assoc-in [:configuration :load :schema] (make-schema schema))
      (assoc-in [:configuration :load :destinationTable] (make-table schema))))

(defn- ^:testable init-resumable-session
  [creds schema content-length]
  (-> (format RESUMABLE-UPLOAD-URL (:project schema))
      (http/post
       {:body (json/encode (configure schema :delimiter "\t"))
        :headers {"X-Upload-Content-Type" "application/octet-stream"
                  "X-Upload-Content-Length" content-length
                  "Content-Type" "application/json; charset=UTF-8"
                  "Authorization" (format "Bearer %s" (auth/access-token creds))}
        :throw-entire-message? true})
      (get-in  [:headers "Location"])))

(defn resumable
  "Resumable upload

  The steps are:
  1. Get resumable session id
  2. Upload file
  3. Handle failures"
  [creds schema path & {:keys [type] :or {type :csv}}]
  (assert (= type :csv))
  ;; Schema should contain columns, table, dataset and project info
  ;; XXX: look into combining that with Google API credentials?
  ;; Terrible way to ensure this, BUT I DON'T CARE RIGHT NOW
  (assert (= #{:project :table :columns :dataset} (set (keys schema))))
  ;; Make sure the file exists
  (assert (.exists (io/file path)) "File doesn't exist")

  ;; Just get a new token every time, that's the stupidest and yet
  ;; most effective way to handle this for now
  ;; Maybe detect type of file here?
  (auth/refresh! creds)
  (let [session-url (init-resumable-session creds schema (.length (io/file path)))
        job (http/put session-url
                      {:body (io/file path)
                       :headers {"Authorization" (format "Bearer %s" (auth/access-token creds))
                                 "Content-Type" "application/octet-stream"}
                       :throw-entire-message? true})]
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
  (:jobs (json/decode (:body
                       (http/get (format "https://www.googleapis.com/bigquery/v2/projects/%s/jobs" project)
                                 {:headers {"Authorization" (format "Bearer %s" (auth/access-token bq))}
                                  :throw-entire-message? true}))
                      true)))
