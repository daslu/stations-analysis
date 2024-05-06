(ns gtfs
  (:require [tablecloth.api :as tc]
            [scicloj.noj.v1.vis.hanami :as hanami]
            [scicloj.noj.v1.vis.stats :as vis.stats]
            [aerial.hanami.templates :as ht]
            [clojure2d.color :as color]
            [scicloj.kindly.v4.kind :as kind]
            [fastmath.clustering :as clustering])
  (:import (org.jgrapht.graph SimpleGraph AsUndirectedGraph DefaultEdge)
           (org.jgrapht.alg.scoring BetweennessCentrality)))

(set! *warn-on-reflection* true)

(def base-path
  "data/gtfs-static/public-transportation/")

(defonce stops
  (-> base-path
      (str "stops.csv.gz")
      (tc/dataset {:key-fn keyword})))

(-> stops
    (tc/rows :as-maps)
    (->> (take 9)))

(def selected-stops
  (-> stops
      (tc/select-rows
       #(and (< 31.9 (:stop_lat %) 32.2)
             (< 34.6 (:stop_lon %) 34.9)))))

(def selected-stop-ids
  (-> selected-stops
      :stop_id
      set))

(defonce stop-times
  (-> base-path
      (str "stop_times.csv.gz")
      (tc/dataset {:key-fn keyword})))

(def edges
  (-> stop-times
      (tc/select-rows
       #(-> % :stop_id selected-stop-ids))
      (tc/group-by [:trip_id] {:result-type :as-seq})
      (->> (mapcat (fn [{:keys [stop_id]}]
                     (map (comp sort vector)
                          stop_id
                          (rest stop_id))))
           (filter (partial apply not=)))
      distinct))

(def vertices
  (->> selected-stops
       :stop_id
       distinct
       sort))

(count edges)
(count vertices)

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
  (-> selected-stops
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


(-> scored-stops
    (tc/map-columns :central [:betweeness] #(> % 0.001))
    (hanami/plot ht/point-chart
                 {:X "stop_lat"
                  :Y "stop_lon"
                  :COLOR "central"
                  :OPACITY 0.1})
    as-geo)

(defn leaflet-map [geojson]
  (delay
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
                                                                               .-style))))}))
                          (.bindTooltip (fn [layer]
                                          (-> layer
                                              .-feature
                                              .-properties
                                              .-tooltip)))
                          (.addTo m))))}])
      {:provider "OpenStreetMap.Mapnik"
       :center   [32 34.8]
       :geojson geojson}]
     {:reagent/deps [:leaflet]})))




(leaflet-map {:type :FeatureCollection
              :features (-> scored-stops
                            (tc/rows :as-maps)
                            (->> (mapv (fn [{:keys [stop_name stop_lat stop_lon betweeness]}]
                                         (let [radius (if (> betweeness 0.001)
                                                        200
                                                        100)
                                               color (if (> betweeness 0.001)
                                                       "purple"
                                                       "darkgreen")]
                                           {:type :Feature
                                            :geometry {:type :Point
                                                       :coordinates [stop_lon stop_lat]}
                                            :properties {:style {:radius radius
                                                                 :fillColor color
                                                                 :color color
                                                                 :weight 1
                                                                 :opacity 1
                                                                 :fillOpacity 0.1}
                                                         :tooltip stop_name}})))))})
