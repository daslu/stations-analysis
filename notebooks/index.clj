(ns index
  (:require [tablecloth.api :as tc]
            [geo
             [geohash :as geohash]
             [jts :as jts]
             [spatial :as spatial]
             [io :as geoio]
             [crs :as crs]]
            [scicloj.noj.v1.vis.hanami :as hanami]
            [aerial.hanami.templates :as ht])
  (:import (org.locationtech.jts.index.strtree STRtree)
           (org.locationtech.jts.geom Geometry Point Polygon Coordinate)
           (org.locationtech.jts.geom.prep PreparedGeometry
                                           PreparedLineString
                                           PreparedPolygon
                                           PreparedGeometryFactory)
           (java.util TreeMap)))

(defonce raw-stations
  (tc/dataset "data/stations20230530.csv.gz"
              {:key-fn keyword
               :separator "|"}))

(tc/shape raw-stations)

(-> raw-stations
    (tc/rows :as-maps)
    first)

(def WGS84 (geo.crs/create-crs 4326))
;; https://epsg.io/?q=Israel
(def Israel1993 (geo.crs/create-crs 2039))

(def crs-transform-Israel1993->WGS84
  (geo.crs/create-transform Israel1993 WGS84))

(def crs-transform-WGS84->Israel1993
  (geo.crs/create-transform WGS84 Israel1993))

(defn Israel1993->WGS84 [geometry]
  (geo.jts/transform-geom geometry crs-transform-Israel1993->WGS84))

(defn WGS84->Israel1993 [geometry]
  (geo.jts/transform-geom geometry crs-transform-WGS84->Israel1993))

(defn yx->point [y x]
  (jts/point (jts/coordinate y x)))

(defn point->yx [^Point point]
  (let [c (.getCoordinate point)]
    [(.getY c)
     (.getX c)]))

(def processed-stations
  (-> raw-stations
      (tc/map-columns :WGS84 [:Long :Lat] yx->point)
      (tc/map-columns :Israel1993 [:WGS84] WGS84->Israel1993)
      (tc/map-columns :yx [:Israel1993] point->yx)
      (tc/map-columns :y [:yx] first)
      (tc/map-columns :x [:yx] second)))


(defn as-geo [vega-lite-spec]
  (-> vega-lite-spec
      (update :encoding update-keys
              #(case %
                 :x :latitude
                 :y :longitude
                 :x2 :latitude2
                 :y2 :longitude2
                 %))
      (assoc :projection {:type :mercator})))


;; Tel Aviv: 32.109333 34.855499

(-> processed-stations
    (tc/select-columns [:CityName :Neighborhood :Lat :Long])
    (tc/select-rows (fn [{:keys [Lat Long]}]
                      (and (< 31.9 Lat 32.2)
                           (< 34.7 Long 35.1))))
    (hanami/plot ht/point-chart
                 {:X :Lat
                  :Y :Long
                  :OPACITY 0.5})
    as-geo
    (assoc-in [:mark :tooltip]
              {:content "data"}))
