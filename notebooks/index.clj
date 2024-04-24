(ns index
  (:require [tablecloth.api :as tc]))

(defonce stations
  (tc/dataset "data/stations20230530.csv.gz"))

(tc/shape stations)
