(ns io.screen6.bigquery.load-test
  (:require
   [io.screen6.bigquery.load :refer :all]
   [midje.sweet :refer :all]
   [midje.util :refer [expose-testables]]))

(expose-testables io.screen6.bigquery.load)

(fact
 "Configuration works correctly"
 (configure nil) => {:configuration {:load {:destinationTable {:projectId nil :datasetId nil :tableId nil}
                                            :schema {:fields ()}
                                            :fieldDelimiter ","}}}
 (keys (get-in (configure nil) [:configuration :load])) => (just #{:fieldDelimiter :destinationTable :schema})
 (get-in (configure nil :delimiter "\t") [:configuration :load :fieldDelimiter]) => "\t"
 (get-in (configure nil :delimiter "\t") [:configuration :load :fieldDelimiter]) => "\t")
