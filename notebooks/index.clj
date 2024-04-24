(ns index
  (:require [tablecloth.api :as tc]
            [aerial.hanami.common :as hc]
            [aerial.hanami.templates :as ht]))

(defonce stations
  (tc/dataset "data/stations20230530.csv.gz"))
