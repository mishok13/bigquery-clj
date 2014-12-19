(ns io.screen6.bigquery.jobs
  "Control creation and execution of Google BigQuery jobs")

(defn submit!
  "Submit a fully configured job to Google BigQuery API"
  [])

(defn all
  "List all jobs from BigQuery"
  [])

(defn status
  "Retrieve job information"
  [])

(defn wait!
  "Wait for the job specified by id to switch to specified status

  Waits for :done status by default"
  [creds job & {:keys [status] :or {status :done}}])
