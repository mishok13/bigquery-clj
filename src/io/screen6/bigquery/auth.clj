(ns io.screen6.bigquery.auth
  (:require
   [clojure.java.io :as io])
  (:import
   [com.google.api.client.googleapis.auth.oauth2 GoogleCredential GoogleCredential$Builder]
   [com.google.api.services.bigquery BigqueryScopes]
   [com.google.api.client.json.jackson2 JacksonFactory]
   [com.google.api.client.googleapis.javanet GoogleNetHttpTransport]))

(defprotocol CredentialProtocol
  (refresh! [this]
    "Refresh the credentials and get new token if needed")
  (access-token [this]
    "Retrieve access token from credentials"))

(defrecord Credential [email scopes subject expires token])

(extend-protocol CredentialProtocol
  GoogleCredential
  (refresh! [this]
    ;; The following line returns true on success, but how do we
    ;; handle failure?
    ;; TODO: figure out failure handling
    (.refreshToken this)
    this)
  (access-token [this]
    (.getAccessToken this))

  Credential
  (refresh! [this] this)
  (access-token [this] (:token this)))

(defn google-credential
  "Google credentials as provided by Java API"
  [email key &
   {:keys [http-transport json-factory scopes]
    :or {http-transport (GoogleNetHttpTransport/newTrustedTransport)
         json-factory (JacksonFactory/getDefaultInstance)
         scopes (BigqueryScopes/all)}}]
  (cond
   (not (.exists (io/file key)))
   (throw (IllegalArgumentException. "Path to PKCS12 key file points to nothing"))

   (not (.endsWith email "@developer.gserviceaccount.com"))
   (throw (IllegalArgumentException. "Email provided has to end with @developer..."))

   :else
   (.. (GoogleCredential$Builder.)
       (setTransport http-transport)
       (setJsonFactory json-factory)
       (setServiceAccountPrivateKeyFromP12File (io/file key))
       (setServiceAccountId email)
       (setServiceAccountScopes scopes)
       (build))))
