(ns index
  (:require [tablecloth.api :as tc]))

(defonce stations
  (tc/dataset "data/stations20230530.csv.gz"
              {:key-fn keyword
               :separator "|"}))

(tc/shape stations)

(-> stations
    (tc/rows :as-maps)
    first)
