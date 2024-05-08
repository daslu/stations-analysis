(ns gtfs
  (:require [tablecloth.api :as tc]
            [tablecloth.column.api :as tcc]
            [scicloj.noj.v1.vis.hanami :as hanami]
            [scicloj.noj.v1.vis.stats :as vis.stats]
            [aerial.hanami.templates :as ht]
            [clojure2d.color :as color]
            [scicloj.kindly.v4.kind :as kind]
            [fastmath.clustering :as clustering]
            [fastmath.core :as fastmath]
            [charred.api :as charred]
            [geo
             [geohash :as geohash]
             [jts :as jts]
             [spatial :as spatial]
             [io :as geoio]
             [crs :as crs]]
            [clojure.java.io :as io])
  (:import (org.jgrapht.graph SimpleGraph AsUndirectedGraph DefaultEdge)
           (org.jgrapht.alg.scoring BetweennessCentrality)
           (org.locationtech.jts.index.strtree STRtree)
           (org.locationtech.jts.geom Geometry Point Polygon Coordinate)
           (org.locationtech.jts.geom.prep PreparedGeometry
                                           PreparedLineString
                                           PreparedPolygon
                                           PreparedGeometryFactory)))



(set! *warn-on-reflection* true)



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



(defn slurp-gzip
  "Read a gzipped file into a string"
  [path]
  (with-open [in (java.util.zip.GZIPInputStream. (clojure.java.io/input-stream path))]
    (slurp in)))


(defonce statistical-areas
  (-> "data/statistical_areas/statistical_areas_2022.geojson.gz"
      slurp-gzip
      geoio/read-geojson))

(def statistical-areas-with-xy
  (->> statistical-areas
       (map #(update % :geometry WGS84->Israel1993))))


(defn make-spatial-index [rows]
  (let [tree (STRtree.)]
    (doseq [{:keys [^Geometry geometry]
             :as row} rows]
      (.insert tree
               (.getEnvelopeInternal geometry)
               (assoc row
                      :prepared-geometry
                      (org.locationtech.jts.geom.prep.PreparedGeometryFactory/prepare geometry))))
    tree))


(def statistical-areas-index
  (make-spatial-index statistical-areas-with-xy))

(defn yx->statistical-areas [yx]
  (let [^Point point (apply yx->point (reverse yx))]
    (.query ^STRtree statistical-areas-index (.getEnvelopeInternal (.buffer point 1)))))

(defn yx->yishuv [yx]
  (->> yx
       yx->statistical-areas
       (map (comp (juxt :SEMEL_YISHUV :SHEM_YISHUV) :properties))
       set))


(def neta-geojson
  (-> "data/neta/LRT_STAT.json.gz"
      slurp-gzip
      (charred/read-json {:key-fn keyword})))



(def neta-stops
  (->> neta-geojson
       :features
       (map-indexed (fn [i feature]
                      (let [line (-> feature
                                     :properties
                                     :description
                                     (#(cond (re-find #"<td>אדום</td>" %) :red
                                             (re-find #"<td>סגול</td>" %) :purple
                                             :else nil)))]
                        {:stop_id (+ 1000000 i)
                         :line line
                         :stop_name (->> feature
                                         :properties
                                         :Name
                                         (str line " - "))
                         :stop_lat (-> feature
                                       :geometry
                                       :coordinates
                                       second)
                         :stop_lon (-> feature
                                       :geometry
                                       :coordinates
                                       first)})))
       tc/dataset))


(def base-path
  "data/gtfs-static/public-transportation/")

(defonce bus-stops
  (-> base-path
      (str "stops.csv.gz")
      (tc/dataset {:key-fn keyword})))

(def all-stops
  (tc/concat bus-stops
             neta-stops))


(def stops-with-location
  (-> all-stops
      (tc/map-columns :WGS84 [:stop_lon :stop_lat] yx->point)
      (tc/map-columns :Israel1993 [:WGS84] WGS84->Israel1993)
      (tc/map-columns :yx [:Israel1993] point->yx)
      (tc/map-columns :y [:yx] first)
      (tc/map-columns :x [:yx] second)
      (tc/map-columns :yishuv [:yx] yx->yishuv)))

(def relevant-stops
  (-> stops-with-location
      (tc/select-rows (fn [{:keys [yishuv]}]
                        (->> yishuv
                             (map first)
                             (some #{681
                                     2400
                                     2520
                                     5000
                                     6100
                                     6200
                                     8600
                                     7900
                                     9400}))))))

(def relevant-stop-ids
  (-> relevant-stops
      :stop_id
      set))


(defonce bus-stop-times
  (-> base-path
      (str "stop_times.csv.gz")
      (tc/dataset {:key-fn keyword})))

(def relevant-stop-times
  (-> bus-stop-times
      (tc/select-rows
       #(-> % :stop_id relevant-stop-ids))))


#_(defn cartesian-product [values]
    (for [v0 values
          v1 values
          :when (< v0 v1)]
      [v0 v1]))



#_(-> relevant-stops
      (tc/add-column :cluster (fn [stops]
                                (-> stops
                                    (tc/select-columns [:x :y])
                                    tc/rows
                                    (clustering/dbscan 1 200)
                                    :clustering)))
      (tc/drop-rows #(-> % :cluster (= 2147483647)))
      (tc/group-by [:cluster] {:result-type :as-seq})
      (->> (mapcat (fn [cluster-stops]
                     (-> cluster-stops
                         :stop_id
                         cartesian-product)))))


(defn distance-based-edges [radius]
  (let [rows (-> relevant-stops
                 (tc/rows :as-maps))]
    (->> rows
         (mapcat (fn [row0]
                   (->> rows
                        (filter (fn [row1]
                                  (and
                                   (not= row0 row1)
                                   (< (fastmath/dist (:yx row0)
                                                     (:yx row1)) radius))))
                        (map (fn [row1]
                               [(:stop_id row0)
                                (:stop_id row1)]))))))))



(def edges
  (-> relevant-stop-times
      (tc/group-by [:trip_id] {:result-type :as-seq})
      (->> (mapcat (fn [{:keys [stop_id]}]
                     (map (comp sort vector)
                          stop_id
                          (rest stop_id))))
           (filter (partial apply not=)))
      (concat (distance-based-edges 250))
      distinct
      vec
      time))

(def vertices
  (->> relevant-stops
       :stop_id
       distinct
       sort))

(count edges)
(count vertices)


(def relevant-stops-lat-lon
  (-> relevant-stops
      (tc/select-columns [:stop_id :stop_lat :stop_lon])
      (tc/set-dataset-name nil)))

(def selectesd-stop-times-lat-lon
  (-> relevant-stop-times
      (tc/left-join relevant-stops-lat-lon [:stop_id])))

(def edges-lat-lon
  (-> edges
      (->> (map (fn [[v0 v1]]
                  {:v0 v0
                   :v1 v1})))
      tc/dataset
      (tc/left-join relevant-stops-lat-lon {:left :v0 :right :stop_id})
      (tc/left-join relevant-stops-lat-lon {:left :v1 :right :stop_id})
      (tc/select-columns [:stop_lat :stop_lon
                          :.stop_lat :.stop_lon])
      (tc/rename-columns {:stop_lat :stop_lat0
                          :stop_lon :stop_lon0
                          :.stop_lat :stop_lat1
                          :.stop_lon :stop_lon1})))



(def graph
  (let [g (SimpleGraph. DefaultEdge)]
    (doseq [v vertices]
      (.addVertex g v))
    (doseq [[v0 v1] edges]
      (.addEdge g v0 v1))
    g))

(def betweeness
  (->> (BetweennessCentrality. graph true)
       .getScores
       (into {})
       time))



(def scored-stops
  (-> relevant-stops
      (tc/map-columns :betweeness [:stop_id] betweeness)
      (tc/log :log-betweeness :betweeness)))

(-> scored-stops
    (vis.stats/histogram :betweeness {:nbins 100}))

(-> scored-stops
    (tc/select-rows #(-> % :betweeness pos?))
    (vis.stats/histogram :log-betweeness {:nbins 100}))


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


(delay
  (-> scored-stops
      (tc/map-columns :central [:betweeness] #(> % 0.002))
      (hanami/plot ht/point-chart
                   {:X "stop_lat"
                    :Y "stop_lon"
                    :COLOR "central"
                    :OPACITY 0.1})
      as-geo))

(defn leaflet-map [geojson]
  (kind/reagent
   ['(fn [{:keys [provider
                  center
                  geojson]}]
       (.log js/console geojson)
       [:div
        {:style {:height "900px"}
         :ref   (fn [el]
                  (let [m (-> js/L
                              (.map el)
                              (.setView (clj->js center)
                                        10))]
                    (-> js/L
                        .-tileLayer
                        (.provider provider)
                        (.addTo m))
                    (-> js/L
                        (.geoJson (clj->js geojson)
                                  ;; https://gist.github.com/geog4046instructor/ac36ea8f317912f51f0bb1d50b5c3481
                                  (clj->js {:pointToLayer (fn [feature latlng]
                                                            (-> js/L
                                                                (.circle latlng
                                                                         (-> feature
                                                                             .-properties
                                                                             .-style))))
                                            :style {:opacity 0.5}}))
                        (.bindTooltip (fn [layer]
                                        (-> layer
                                            .-feature
                                            .-properties
                                            .-tooltip)))
                        (.addTo m))))}])
    {:provider "Stadia.AlidadeSmoothDark"
     :center   [32 34.8]
     :geojson geojson}]
   {:reagent/deps [:leaflet]}))




(delay
  (leaflet-map
   {:type :FeatureCollection
    :features
    (vec
     (concat (-> edges-lat-lon
                 (tc/rows :as-maps)
                 (->> (mapv (fn [{:keys [stop_lat0 stop_lon0 stop_lat1 stop_lon1]}]
                              {:type :Feature
                               :geometry {:type :LineString
                                          :coordinates [[stop_lon0 stop_lat0]
                                                        [stop_lon1 stop_lat1]]}}))))
             (-> scored-stops
                 (tc/rows :as-maps)
                 (->> (mapv (fn [{:keys [stop_name stop_lat stop_lon betweeness]}]
                              (let [radius (if (> betweeness 0.002)
                                             50
                                             20)
                                    color (if (> betweeness 0.002)
                                            "yellow"
                                            "cyan")]
                                {:type :Feature
                                 :geometry {:type :Point
                                            :coordinates [stop_lon stop_lat]}
                                 :properties {:style {:radius radius
                                                      :fillColor color
                                                      :color color
                                                      :weight 1
                                                      :opacity 1
                                                      :fillOpacity 0.5}
                                              :tooltip (str stop_name " " (format "%.02f%%" (* 100 betweeness)))}})))))))}))
