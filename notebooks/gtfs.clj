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
            [clojure.reflect :as reflect]
            [tech.v3.dataset.print :as print]
            [geo
             [geohash :as geohash]
             [jts :as jts]
             [spatial :as spatial]
             [io :as geoio]
             [crs :as crs]]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [tech.parallel :as parallel])
  (:import (org.jgrapht.graph SimpleGraph SimpleWeightedGraph
                              SimpleDirectedWeightedGraph
                              AsUndirectedGraph
                              DefaultEdge DefaultWeightedEdge)
           (org.jgrapht.alg.scoring BetweennessCentrality)
           org.jgrapht.alg.shortestpath.DijkstraShortestPath
           (org.locationtech.jts.index.strtree STRtree)
           (org.locationtech.jts.geom Geometry Point Polygon Coordinate)
           (org.locationtech.jts.geom.prep PreparedGeometry
                                           PreparedLineString
                                           PreparedPolygon
                                           PreparedGeometryFactory)
           org.jgrapht.alg.util.Pair))



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
    (.query ^STRtree statistical-areas-index (.getEnvelopeInternal (.buffer point 100)))))

(defn yx->yishuv [yx]
  (->> yx
       yx->statistical-areas
       (map (comp (juxt :SEMEL_YISHUV :SHEM_YISHUV) :properties))
       set))


(def dankal-geojson
  (-> "data/dankal/LRT_STAT.json.gz"
      slurp-gzip
      (charred/read-json {:key-fn keyword})))


#_(->> dankal-geojson
       :features
       (map (fn [feature]
              (let [line (-> feature
                             :properties
                             :description
                             (#(cond (re-find #"<td>אדום</td>" %) "דנקל - אדום"
                                     (re-find #"<td>סגול</td>" %) "דנקל - סגול"
                                     :else nil)))]
                {:line line
                 :name (-> feature :properties :Name)}))))


(def dankal-stops
  (->> dankal-geojson
       :features
       (map-indexed (fn [i feature]
                      (let [line (-> feature
                                     :properties
                                     :description
                                     (#(cond (re-find #"<td>אדום</td>" %) "דנקל - אדום"
                                             (re-find #"<td>סגול</td>" %) "דנקל - סגול"
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
       (filter :line)
       tc/dataset))

(def dankal-name->stop
  (-> dankal-stops
      (tc/rows :as-maps)
      (->> (filter :line)
           (map (juxt :stop_name :stop_id))
           (into {}))))

(defn seq->pairs [s]
  (map vector
       s
       (rest s)))

(defn seq->double-pairs [s]
  (->> s
       seq->pairs
       (mapcat (juxt identity reverse))))

#_(-> dankal-stops
      (tc/select-rows #(-> % :line (= "דנקל - אדום")))
      :stop_name
      vec)

(def dankal-edges
  (->> [["דנקל - סגול - הטייסים"
         "דנקל - סגול - אלטלף"
         "דנקל - סגול - ויצמן"
         "דנקל - סגול - יהוד מרכז"
         "דנקל - סגול - יהוד מערב"
         "דנקל - סגול - סביונים"
         "דנקל - סגול - מונוסון"
         "דנקל - סגול - צומת סביון"
         "דנקל - סגול - נוה סביון"
         "דנקל - סגול - יגאל אלון"
         "דנקל - סגול - קזז"
         "דנקל - סגול - רמת פנקס"
         "דנקל - סגול - מסובים"
         "דנקל - סגול - אפעל דרום"
         "דנקל - סגול - שיבא"
         "דנקל - סגול - אפעל צפון"
         "דנקל - סגול - אלוף שדה"
         "דנקל - סגול - הירדן"
         "דנקל - סגול - רזיאל"
         "דנקל - סגול - כורזין"
         "דנקל - סגול - המאבק"
         "דנקל - סגול - פארק וולפסון"
         "דנקל - סגול - משה דיין"
         "דנקל - סגול - גבעתי"
         "דנקל - סגול - האצ\"ל"
         "דנקל - סגול - ההגנה"
         "דנקל - סגול - נוה שאנן"
         "דנקל - סגול - הר ציון"
         "דנקל - סגול - העליה"
         "דנקל - סגול - יהודה הלוי"
         "דנקל - סגול - מונטיפיורי"
         "דנקל - סגול - הכרמל"
         "דנקל - סגול - מוגרבי"
         "דנקל - סגול - בוגרשוב"
         "דנקל - סגול - גורדון"
         "דנקל - סגול - דיזנגוף"
         "דנקל - סגול - אבן גבירול"
         "דנקל - סגול - איכילוב"
         "דנקל - סגול - מסוף ארלוזורוב"]
        ["דנקל - סגול - בר אילן מזרח"
         "דנקל - סגול - בר אילן מערב"
         "דנקל - סגול - גני אילן"
         "דנקל - סגול - גורן"
         "דנקל - סגול - אפעל צפון"]
        ["דנקל - אדום - הקוממיות"
         "דנקל - אדום - העמל"
         "דנקל - אדום - כ\"ט בנובמבר"
         "דנקל - אדום - יוספטל"
         "דנקל - אדום - בנימין"
         "דנקל - אדום - בלפור"
         "דנקל - אדום - זבוטנסקי"
         "דנקל - אדום - רוטשילד"
         "דנקל - אדום - העצמאות"
         "דנקל - אדום - מחרוזת"
         "דנקל - אדום - הבעש\"ט"
         "דנקל - אדום - איסקוב"
         "דנקל - אדום - ארליך"
         "דנקל - אדום - בן צבי"
         "דנקל - אדום - סלמה"
         "דנקל - אדום - אליפלט"
         "דנקל - אדום - אלנבי"
         "דנקל - אדום - קרליבך"
         "דנקל - אדום - יהודית"
         "דנקל - אדום - שאול המלך"
         "דנקל - אדום - ארלוזורוב"
         "דנקל - אדום - אבא הלל"
         "דנקל - אדום - ביאליק"
         "דנקל - אדום - בן גוריון"
         "דנקל - אדום - אהרונוביץ"
         "דנקל - אדום - שנקר"
         "דנקל - אדום - שחם"
         "דנקל - אדום - בילינסון"
         "דנקל - אדום - דנקנר"
         "דנקל - אדום - קרול"
         "דנקל - אדום - פינסקר"
         "דנקל - אדום - תחנה מרכזית פתח"]
        ["דנקל - אדום - אהרונוביץ"
         "דנקל - אדום - אם המושבות"
         "דנקל - אדום - קרית אריה"]]
       (map (partial map dankal-name->stop))
       (mapcat seq->double-pairs)))

(def base-path
  "data/gtfs-static/public-transportation/")

(defonce bus-stops
  (-> base-path
      (str "stops.csv.gz")
      (tc/dataset {:key-fn keyword})))

(def all-stops
  (tc/concat bus-stops
             dankal-stops))


(def stops-with-location
  (-> all-stops
      (tc/map-columns :WGS84 [:stop_lon :stop_lat] yx->point)
      (tc/map-columns :Israel1993 [:WGS84] WGS84->Israel1993)
      (tc/map-columns :yx [:Israel1993] point->yx)
      (tc/map-columns :y [:yx] first)
      (tc/map-columns :x [:yx] second)
      (tc/map-columns :yishuv [:yx] yx->yishuv)))

(def stops-in-region
  (-> stops-with-location
      (tc/select-rows (fn [{:keys [yishuv]}]
                        (->> yishuv
                             (map first)
                             (some #{681
                                     2400
                                     2620
                                     5000
                                     6100
                                     6200
                                     8600
                                     7900
                                     9400}))))))


(def stop-ids-in-region
  (-> stops-in-region
      :stop_id
      set))

(delay
  (-> stops-in-region
      :yishuv
      (->> (mapcat (partial map first))
           frequencies
           (sort-by second)
           reverse)))


(defonce bus-stop-times
  (-> base-path
      (str "stop_times.csv.gz")
      (tc/dataset {:key-fn keyword})))

(comment
  (-> bus-stop-times
      (print/print-range :all)
      (tc/head 1000)))


(def relevant-stop-times
  (-> bus-stop-times
      (tc/group-by :trip_id)
      (tc/without-grouping->
       (tc/select-rows
        (fn [row]
          (->> row
               :data
               :stop_id
               (some stop-ids-in-region)))))
      (tc/order-by [:stop_sequence])
      (tc/ungroup)
      time))


(tc/row-count bus-stop-times)
(tc/row-count relevant-stop-times)

(def relevant-stop-ids
  (-> relevant-stop-times
      :stop_id
      (concat stop-ids-in-region)
      set))


(count stop-ids-in-region)
(count relevant-stop-ids)

(def relevant-stops
  (-> stops-with-location
      (tc/select-rows #(-> % :stop_id relevant-stop-ids))))

(delay
  (-> relevant-stops
      :line
      frequencies))


(def distance-based-edges
  (memoize (fn [radius]
             (let [rows (-> relevant-stops
                            (tc/rows :as-maps))]
               (->> rows
                    vec
                    (parallel/queued-pmap
                     32
                     (fn [row0]
                       (->> rows
                            (into
                             []
                             (comp (filter (fn [row1]
                                             (and
                                              (< (:stop_id row0)
                                                 (:stop_id row1))
                                              (< (fastmath/dist (:yx row0)
                                                                (:yx row1)) radius))))
                                   (map (fn [row1]
                                          [(:stop_id row0)
                                           (:stop_id row1)])))))))
                    (apply concat)
                    (mapcat (juxt identity reverse)))))))

(delay
  (->> [0 1 100 250]
       (map distance-based-edges)
       (mapv count)
       time))


(def bus-edges
  (-> relevant-stop-times
      (tc/group-by [:trip_id] {:result-type :as-seq})
      (->> (mapcat (fn [{:keys [stop_id]}]
                     (seq->pairs stop_id)))
           (filter (partial apply not=))
           distinct)))

(count bus-edges)

(delay
  (->> bus-edges
       (apply concat)
       (map (comp some? stop-ids-in-region))
       frequencies))

(delay
  (->> bus-edges
       (apply concat)
       (map (comp some? relevant-stop-ids))
       frequencies))


(def vertices
  (memoize (fn [dankal-lines]
             (-> relevant-stops
                 (tc/select-rows (fn [{:keys [line]}]
                                   (if line
                                     (dankal-lines line)
                                     true)))
                 :stop_id
                 set))))


(defn weighted-edge [w] (fn [e] [w e]))


(def edges
  (memoize (fn [dankal-lines]
             (let [vs (vertices dankal-lines)]
               (-> (concat (map (weighted-edge 1)
                                (distance-based-edges 250))
                           (map (weighted-edge 1)
                                bus-edges)
                           (map (weighted-edge 0.5)
                                dankal-edges))
                   (->> (group-by second))
                   (update-vals (fn [edges]
                                  (->> edges
                                       (sort-by first)
                                       first)))
                   vals
                   (->> (filter (fn [[w [v0 v1]]]
                                  (and (vs v0)
                                       (vs v1))))))))))

(count (vertices #{}))
(count (edges #{}))
(count (vertices #{"דנקל - אדום"}))
(count (edges #{"דנקל - אדום"}))
(count (vertices #{"דנקל - אדום" "דנקל - סגול"}))
(count (edges #{"דנקל - אדום" "דנקל - סגול"}))


(delay
  (->> (edges #{"דנקל - אדום" "דנקל - סגול"})
       (map (fn [[_ vs]]
              [(some? (some #(< % 1000000) vs))
               (some? (some #(>= % 1000000) vs))]))
       frequencies))


(def relevant-stops-lat-lon
  (-> relevant-stops
      (tc/select-columns [:stop_id :stop_lat :stop_lon :yx :stop_name])
      (tc/set-dataset-name nil)))


(defn edges-details [dankal-lines]
  (-> dankal-lines
      edges
      (->> (map (fn [[w [v0 v1]]]
                  {:w w
                   :v0 v0
                   :v1 v1})))
      tc/dataset
      (tc/left-join relevant-stops-lat-lon {:left :v0 :right :stop_id})
      (tc/left-join relevant-stops-lat-lon {:left :v1 :right :stop_id})
      (tc/map-columns :yx-distance [:yx :.yx] fastmath/dist)
      (tc/select-columns [:stop_lat :.stop_lat
                          :stop_lon :.stop_lon
                          :stop_id :.stop_id
                          :stop_name :.stop_name
                          :w :yx-distance])
      (tc/rename-columns {:stop_lat :stop_lat0
                          :stop_lon :stop_lon0
                          :.stop_lat :stop_lat1
                          :.stop_lon :stop_lon1
                          :stop_id :stop_id0
                          :.stop_id :stop_id1
                          :stop_name :stop_name0
                          :.stop_name :stop_name1})))


(delay (edges-details #{"דנקל - אדום" "דנקל - סגול"}))


(delay
  (-> #{}
      edges-details
      (tc/select-rows #(-> % :w (= 1)))
      (vis.stats/histogram :yx-distance {:nbins 100})))


(delay
  (-> #{}
      edges-details
      (tc/select-rows #(and (-> % :w (= 1))
                            (-> % :yx-distance (> 2000))))))

(delay
  (-> #{}
      edges-details
      (tc/select-rows #(-> % :stop_id0 (= 13807)))))

(delay
  (-> bus-stop-times
      (tc/group-by [:trip_id] {:result-type :as-seq})
      (->> (filter (fn [ds]
                     (->> ds
                          :stop_id
                          (filter #{13192 29521})
                          count
                          (= 2))))
           (filter (fn [ds]
                     (-> ds
                         (tc/select-rows
                          #(-> % :stop_id (#{13192 29521})))
                         :stop_sequence
                         (= [34 36]))))
           (map #(print/print-range % :all)))))


(delay
  (-> relevant-stop-times
      (tc/group-by [:trip_id] {:result-type :as-seq})
      (->> (filter (fn [ds]
                     (->> ds
                          :stop_id
                          (filter #{13192 29521})
                          count
                          (= 2))))
           (filter (fn [ds]
                     (-> ds
                         (tc/select-rows
                          #(-> % :stop_id (#{13192 29521})))
                         :stop_sequence
                         (= [34 36]))))
           (map (fn [ds]
                  (-> ds
                      (tc/left-join bus-stops [:stop_id])
                      (tc/order-by [:stop_sequence]))))
           ;; (map (fn [{:keys [stop_id]}]
           ;;        [(vec stop_id)
           ;;         (seq->pairs stop_id)]))
           )
      kind/fragment))


(delay
  (-> relevant-stop-times
      (tc/select-rows #(-> % :trip_id (= "58029926_020524")))
      (tc/left-join bus-stops :stop_id)
      (print/print-range :all)))


(delay (->> bus-edges
            (filter #(= % [13192 29521]))))

(def graph
  (memoize (fn [dankal-lines]
             (let [g (SimpleDirectedWeightedGraph. DefaultWeightedEdge)]
               (doseq [v (vertices dankal-lines)]
                 (.addVertex g v))
               (doseq [[w [v0 v1]] (edges dankal-lines)]
                 (let [e (.addEdge g v0 v1)]
                   (.setEdgeWeight
                    g
                    e
                    w)))
               g))))


(delay
  (-> #{}
      edges
      (->> (map (fn [[w [v0 v1]]]
                  {:w w
                   :v0 v0
                   :v1 v1})))
      tc/dataset
      (tc/left-join relevant-stops-lat-lon {:left :v0 :right :stop_id})
      (tc/left-join relevant-stops-lat-lon {:left :v1 :right :stop_id})))



;; 13320 Azrieli
;; 50379 Petah Tikva Merkazit
;; 48920 Savidor


(delay
  (->> [#{} #{"דנקל - אדום"}]
       (map (fn [dankal-lines]
              [dankal-lines
               (let [stop-id->name (-> relevant-stops
                                       (tc/select-columns [:stop_id :stop_name])
                                       tc/rows
                                       (->> (into {})))
                     predecessors (-> dankal-lines
                                      graph
                                      (DijkstraShortestPath.)
                                      (.getPaths 50379)
                                      (.getDistanceAndPredecessorMap)
                                      (->> (into {}))
                                      (update-vals (fn [^Pair p]
                                                     [(.getFirst p)
                                                      (-> p
                                                          (.getSecond)
                                                          str
                                                          (str/replace #"[\(| |\)]" "")
                                                          (str/split #":")
                                                          (->> (mapv #(try (Integer/parseInt %)
                                                                           (catch Exception e nil)))))])))]
                 (-> (loop [v 48920
                            story []]
                       (let [[distance [next-v _]] (predecessors v)]
                         (if (< distance 1)
                           story
                           (recur next-v
                                  (conj story {:stop_id v
                                               :stop_name (stop-id->name v)
                                               :distance distance})))))
                     reverse
                     tc/dataset))]))
       kind/fragment))






(def score
  (memoize
   (fn [dankal-lines target-stop-id]
     (-> dankal-lines
         graph
         (DijkstraShortestPath.)
         (.getPaths target-stop-id)
         (.getDistanceAndPredecessorMap)
         (->> (into {}))
         (update-vals (fn [^Pair pair]
                        (.getFirst pair)))))))




(delay
  (-> (->> [#{}
            #{"דנקל - אדום" "דנקל - סגול"}]
           (map (fn [dankal-lines]
                  (-> dankal-lines
                      (score 50379)
                      (->> (map (fn [[stop_id score]]
                                  {:stop_id stop_id
                                   :score score})))
                      tc/dataset)))
           (apply #(tc/left-join %1 %2 [:stop_id])))
      (tc/map-columns :eq [:score :right.score] =)
      :eq
      frequencies))


(defn betweeness [dankal-lines]
  (-> dankal-lines
      graph
      (BetweennessCentrality. true)
      .getScores
      (->> (into {}))
      time))


(delay
  (-> (->> [#{} #{"דנקל - אדום" "דנקל - סגול"}]
           (map (fn [dankal-lines]
                  (-> dankal-lines
                      (score 50379)
                      (->> (map (fn [[stop_id score]]
                                  {:stop_id stop_id
                                   :score score})))
                      tc/dataset)))
           (apply #(tc/left-join %1 %2 [:stop_id])))
      (tc/map-columns :eq [:score :right.score] =)
      :eq
      frequencies))


(def scored-stops
  (memoize (fn [dankal-lines target-stop-id]
             (-> relevant-stops
                 ;; (tc/map-columns :betweeness [:stop_id] (betweeness dankal-lines))
                 ;; (tc/log :log-betweeness :betweeness)
                 (tc/map-columns :score [:stop_id] (score dankal-lines target-stop-id))))))

(delay
  (-> #{"דנקל - אדום" "דנקל - סגול"}
      (scored-stops 50379)
      (vis.stats/histogram :betweeness {:nbins 100})))

(delay
  (-> #{"דנקל - אדום" "דנקל - סגול"}
      (scored-stops 50379)
      (tc/select-rows #(-> % :betweeness pos?))
      (vis.stats/histogram :log-betweeness {:nbins 100})))


(delay
  (-> #{"דנקל - אדום" "דנקל - סגול"}
      (scored-stops 50379)
      (tc/group-by [:score])
      (tc/aggregate {:n tc/row-count})
      (hanami/plot ht/bar-chart {:X :score
                                 :Y :n})))

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
  (-> #{"דנקל - אדום" "דנקל - סגול"}
      (scored-stops 50379)
      (tc/map-columns :close [:score] #(some-> % (< 6)))
      (hanami/plot ht/point-chart
                   {:X "stop_lat"
                    :Y "stop_lon"
                    :COLOR "close"
                    :OPACITY 0.1})
      as-geo))

(defn leaflet-map [geojson]
  (kind/reagent
   ['(fn [{:keys [provider
                  center
                  geojson]}]
       (.log js/console geojson)
       [:div
        {:style {:height "400px"}
         :ref   (fn [el]
                  (let [m (-> js/L
                              (.map el)
                              (.setView (clj->js center)
                                        11))]
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


(def grad
  (color/gradient [:red :yellow]))



(defn stops-map [dankal-lines target-stop-id]
  (leaflet-map
   {:type :FeatureCollection
    :features
    (vec
     (concat (-> dankal-lines
                 (scored-stops target-stop-id)
                 (tc/rows :as-maps)
                 (->> (mapv (fn [{:keys [stop_id stop_name stop_lat stop_lon score]}]
                              (let [radius 50
                                    color (or (some-> score
                                                      (/ 20)
                                                      grad
                                                      color/format-hex)
                                              "black")]
                                {:type :Feature
                                 :geometry {:type :Point
                                            :coordinates [stop_lon stop_lat]}
                                 :properties {:style {:radius radius
                                                      :fillColor color
                                                      :color color
                                                      :weight 1
                                                      :opacity 1
                                                      :fillOpacity 0.5}
                                              :tooltip (str stop_id
                                                            " "
                                                            stop_name
                                                            " "
                                                            (some->> score
                                                                     (format "%.0f")))}})))))
             (-> dankal-lines
                 edges-details
                 (tc/select-rows #(and (-> % :stop_id0 (> 1000000))
                                       (-> % :stop_id1 (> 1000000)))) ; dankal
                 (tc/rows :as-maps)
                 (->> (mapv (fn [{:keys [stop_lat0 stop_lon0 stop_lat1 stop_lon1]}]
                              {:type :Feature
                               :geometry {:type :LineString
                                          :coordinates [[stop_lon0 stop_lat0]
                                                        [stop_lon1 stop_lat1]]}}))))))}))



(kind/fragment
 [(stops-map #{"דנקל - אדום" "דנקל - סגול"} 50379)
  (stops-map #{} 50379)])







:bye
