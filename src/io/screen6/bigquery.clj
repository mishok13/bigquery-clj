(ns io.screen6.bigquery
  "Google BigQuery API wrapper implemented in Clojure"
  (:require [clojure.tools.logging :as logging])
  (:import
   [com.google.api.client.googleapis.auth.oauth2
    GoogleClientSecrets GoogleCredential GoogleCredential$Builder]
   [com.google.api.client.googleapis.javanet GoogleNetHttpTransport]
   [com.google.api.client.googleapis.json GoogleJsonResponseException]
   [com.google.api.client.json.jackson2 JacksonFactory]
   [com.google.api.client.util Data]
   [com.google.api.services.bigquery
    BigqueryScopes Bigquery$Jobs$Insert Bigquery Bigquery$Builder]
   [com.google.api.services.bigquery.model
    Job GetQueryResultsResponse JobConfiguration JobConfigurationQuery
    JobReference TableReference TableDataInsertAllRequest$Rows TableRow
    TableDataInsertAllRequest TableDataInsertAllResponse]))

;; Forward-declaring private functions
(declare authorize!)
(declare connect!)
(declare make-query)
(declare set-row)
(declare prepare-job)
(declare job-configuration)

(deftype Query [^Bigquery bq ^JobReference job m]
  clojure.lang.IObj
  (meta [_] m)
  (withMeta [_ m] (Query. bq job m))
  Object
  (toString [this]
            (str (.getName (class this)) ": " (pr-str job)))
  clojure.lang.IPending
  (isRealized [_]
    (logging/debugf "Check status of job %s" (.getJobId job))
    (-> bq
        (.jobs)
        (.get (.getProjectId job) (.getJobId job))
        (.execute)
        (.getStatus)
        (.getState)
        (.equals "DONE")))
  clojure.lang.IDeref
  (deref [this]
    (let [max-sleep 15000]
      (loop [sleep 250]
        (if (realized? this)
          (do
            (logging/debugf "Returning query results of job %s" (.getJobId job))
            (-> bq
                (.jobs)
                (.getQueryResults (.getProjectId job) (.getJobId job))
                (.execute)))
          (do
            ;; Exponentially increase polling interval for complicated
            ;; jobs, to avoid API saturation
            (logging/debugf "Job %s waiting for %s milliseconds" (.getJobId job) sleep)
            (Thread/sleep sleep)
            (recur (min (long (* 1.618 sleep)) max-sleep))))))))

(defprotocol PBigQueryConnection
  (datasets [this]
    "List datasets available")
  (switch-dataset [this dataset]
    "Switch connection to a different dataset")
  (tables [this]
    "List tables available in current dataset")
  (execute! [this sql] [this sql table] [this sql table opts]
    "Execute the query and optionally write the result to a table")
  (table-exists? [this table]
    "Check if the table exists")
  (insert! [this table row]))

(defrecord BigQueryConnection [connection project dataset]
  PBigQueryConnection
  (datasets [this]
    (->> (.. connection (datasets) (list project) (execute) (getDatasets))
         (filter (fn bigquery-dataset? [info] (= "bigquery#dataset" (get info "kind"))))
         (map (fn simple-dataset-name [info] (get-in info ["datasetReference" "datasetId"])))))
  (tables [this]
    (->> (.. connection (tables) (list project dataset) (execute) (getTables))
         (filter (fn bigquery-table? [info] (= "bigquery#table" (get info "kind"))))
         (map (fn simple-table-name [info] (get-in info ["tableReference" "tableId"])))))
  (insert! [this table row]
    (let [rows (doto (TableDataInsertAllRequest$Rows.)
                 (.setInsertId (str (System/nanoTime)))
                 (.setJson (set-row row)))
          content (.setRows (TableDataInsertAllRequest.) (java.util.ArrayList. [rows]))]
      (.. connection (tabledata) (insertAll project dataset table content) (execute))))
  (execute! [this sql]
    (let [results (deref (make-query connection project sql))
          fields (map keyword (map (fn get-name [m] (get m "name")) (get-in results ["schema" "fields"])))]
      (->> (get results "rows")
           ;; FIXME: this is clearly not worthy a function, but maybe
           ;; there's a better way to avoid such deeply nested
           ;; approach
           (map (fn get-row [row]
                  (map (fn get-value [cell]
                         (let [value (get cell "v")]
                           ;; This is beyond anything that I can
                           ;; grasp, but for some reason Google's Java
                           ;; API needs a way to distinguish null
                           ;; values as opposed to simply missing JSON
                           ;; entries. This is barely mentioned in
                           ;; their doc, without any real explanation.
                           (when-not (Data/isNull value) value)))
                       (get row "f"))))
           (map (partial zipmap fields))))))

(defn bigquery
  "Create a BigQuery connection

  \"account\" can be found in \"credentials\" section, under \"Email
  address\" field. \"key\" should be a path to P12 key. Project and dataset specify which "
  [account key project dataset]
  ;; TODO: check connection, check if key path can be read, check if
  ;; project and dataset are correct
  (let [connection (connect! account key)]
    (BigQueryConnection. connection project dataset)))

(defn authorize!
  "Authorize the API user"
  [account key-path &
   {:keys [http-transport json-factory]
    :or {http-transport (GoogleNetHttpTransport/newTrustedTransport)
         json-factory (JacksonFactory/getDefaultInstance)}}]
  (-> (GoogleCredential$Builder.)
      (.setTransport http-transport)
      (.setJsonFactory json-factory)
      (.setServiceAccountPrivateKeyFromP12File (clojure.java.io/file key-path))
      (.setServiceAccountId account)
      (.setServiceAccountScopes (BigqueryScopes/all))
      (.build)))

(defn ^:private connect!
  "Connect to BigQuery under given account"
  [account key-path]
  (let [json-factory (JacksonFactory/getDefaultInstance)
        http-transport (GoogleNetHttpTransport/newTrustedTransport)]
    (.build (Bigquery$Builder. http-transport
                               json-factory
                               (authorize! account key-path
                                           :json-factory json-factory
                                           :http-transport http-transport)))))

(defn ^:private job-configuration
  [sql & {:keys [table use-cache? flatten-results? large-results? table-mode]}]
  (let [conf-query (JobConfigurationQuery.)]
    (cond-> (.setQuery conf-query sql)
            large-results? (.setAllowLargeResults (boolean large-results?))
            use-cache? (.setUseQueryCache (boolean use-cache?)))))

;; (defn ^:private prepare-job
;;   [project dataset sql table table-mode allow-large-results? use-cached-results?]
;;   (logging/debugf "Preparing job for SQL '%s' with output table '%s'" sql table)
;;   (let [table (when table
;;                 (-> (TableReference.)
;;                     (.setProjectId project)
;;                     (.setDatasetId dataset)
;;                     (.setTableId table)))
;;         write-disposition (get {:overwrite "WRITE_TRUNCATE"
;;                                 :append "WRITE_APPEND"
;;                                 :if-empty "WRITE_EMPTY"}
;;                                table-mode)]
;;     (->> sql
;;          ;; Overly complicated setup for
;;          ;; JobConfigurationQuery. Unfortunately, it's required in
;;          ;; this case, since the somewhat strange way of setting up
;;          ;; the whole job.
;;          (#(cond-> (-> (JobConfigurationQuery.)
;;                        (.setQuery %)
;;                        (.setAllowLargeResults (boolean allow-large-results?))
;;                        (.setUseQueryCache (boolean use-cached-results?))
;;                        (.setWriteDisposition write-disposition)
;;                        (.setFlattenResults true))
;;                    table (.setDestinationTable table)))
;;          ;; The only single purpose of this class is to wrap a query
;;          ;; configuration. It's true and it pains me, Google APIs are
;;          ;; just completely developer-unfriendly.
;;          (#(doto (JobConfiguration.) (.setQuery %)))
;;          (#(doto (Job.) (.setConfiguration %))))))

(defn ^:private prepare-job
  ([sql]
   (-> sql
       (job-configuration)
       (#(doto (JobConfiguration.) (.setQuery %)))
       (#(doto (Job.) (.setConfiguration %))))))

(defn ^:private make-query
  "Start a query and return a value which will return a "
  [^Bigquery bq ^String project ^String sql]
  (let [job (prepare-job sql)]
    (Query. bq
            (-> bq
                (.jobs)
                (.insert project job)
                (.execute)
                (.getJobReference))
            nil)))

(defn ^:private set-row
  "Prepare a row for insertion into BigQuery"
  [row]
  (let [table-row (TableRow.)]
    (doseq [[key value] row]
      (.set table-row key value))
    table-row))
