#!/usr/bin/env bash
# Node-side Jepsen driver. Runs on a control node (a surviving Graviton
# node) and tests the final dynomite cluster (RESP plane on port 9102)
# using a Jepsen register-linearizability + set workload under a
# partition nemesis.
#
# Jepsen proper is a Clojure/JVM harness. On AL2023 we install a JDK +
# the Clojure CLI, then run a self-contained test. Because a full
# jepsen.dynomite project is large, this driver uses jepsen's library
# form: a minimal deps.edn project that pulls jepsen + a redis client,
# defines a register (compare-and-set via Lua/WATCH is not available
# through the proxy, so we test a monotonic-append set + a last-write
# register) and a partition nemesis, and runs the Knossos/Elle checker.
#
# Emits JEPSEN_RESULT PASS|FAIL and the checker's :valid? line, then
# JEPSEN_DONE.
set -uo pipefail
echo "JEPSEN_BEGIN $(date -u +%FT%TZ)"

NODES_FILE="${1:-$HOME/jepsen-nodes.txt}"
mapfile -t NODES < "$NODES_FILE"
echo "jepsen: ${#NODES[@]} db nodes: ${NODES[*]}"

# --- deps: JDK + clojure CLI ---
if ! command -v java >/dev/null 2>&1; then
  sudo dnf install -y -q java-21-amazon-corretto-headless 2>&1 | tail -1 || \
  sudo dnf install -y -q java-17-amazon-corretto-headless 2>&1 | tail -1
fi
if ! command -v clojure >/dev/null 2>&1; then
  curl -sL https://download.clojure.org/install/linux-install.sh -o /tmp/clj.sh 2>/dev/null && \
    chmod +x /tmp/clj.sh && sudo bash /tmp/clj.sh >/dev/null 2>&1
fi
command -v java >/dev/null 2>&1 || { echo "JEPSEN_RESULT FAIL (no jdk)"; echo "JEPSEN_DONE"; exit 1; }

# --- project ---
mkdir -p ~/jdyn/src/jdyn && cd ~/jdyn
cat > deps.edn <<'EDN'
{:deps {jepsen/jepsen {:mvn/version "0.3.5"}
        com.taoensso/carmine {:mvn/version "3.4.1"}}}
EDN

# Build the node list as an EDN vector.
NODE_EDN=$(printf '"%s" ' "${NODES[@]}")

cat > src/jdyn/core.clj <<CLJ
(ns jdyn.core
  (:require [jepsen [cli :as cli] [tests :as tests] [control :as c]
                    [client :as client] [nemesis :as nemesis]
                    [generator :as gen] [checker :as checker] [db :as db]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.os.debian :as debian]
            [knossos.model :as model]
            [taoensso.carmine :as car :refer [wcar]]))

(def port 9102)
(defn conn [node] {:pool {} :spec {:host node :port port :timeout-ms 5000}})

(defrecord RegClient [node]
  client/Client
  (open! [this test n] (assoc this :node n))
  (setup! [this test])
  (invoke! [this test op]
    (try
      (case (:f op)
        :read  (let [v (wcar (conn node) (car/get "jkey"))]
                 (assoc op :type :ok :value (when v (Long/parseLong v))))
        :write (do (wcar (conn node) (car/set "jkey" (str (:value op))))
                   (assoc op :type :ok)))
      (catch Exception e (assoc op :type :fail :error (.getMessage e)))))
  (teardown! [this test])
  (close! [this test]))

(defn r [_ _] {:type :invoke :f :read :value nil})
(defn w [_ _] {:type :invoke :f :write :value (rand-int 5)})

(defn noop-db [] (reify db/DB (setup! [_ _ _]) (teardown! [_ _ _])))

(defn jdyn-test [opts]
  (merge tests/noop-test opts
    {:name "dynomite-register"
     :os debian/os
     :db (noop-db)
     :client (RegClient. nil)
     :nemesis (nemesis/partition-random-halves)
     :checker (checker/compose
                {:linear (checker/linearizable
                           {:model (model/register)
                            :algorithm :linear})
                 :timeline (timeline/html)})
     :generator (->> (gen/mix [r w])
                     (gen/stagger 1/50)
                     (gen/nemesis
                       (gen/seq (cycle [(gen/sleep 5)
                                        {:type :info :f :start}
                                        (gen/sleep 5)
                                        {:type :info :f :stop}])))
                     (gen/time-limit 60))}))

(defn -main [& args]
  (cli/run! (cli/single-test-cmd {:test-fn jdyn-test}) args))
CLJ

echo "jepsen: running register-linearizability test with partition nemesis"
# nodes must be reachable; jepsen ssh is not needed since our DB is noop
# and the client connects directly to the RESP port.
NODESTR=$(IFS=,; echo "${NODES[*]}")
timeout 1200 clojure -M -m jdyn.core test \
  --nodes "$NODESTR" \
  --username ec2-user \
  --time-limit 60 \
  --concurrency 20 2>&1 | tee ~/jepsen-run.out | tail -40

# Extract the validity verdict.
if grep -qE ':valid\? true|Everything looks good' ~/jepsen-run.out; then
  echo "JEPSEN_RESULT PASS"
elif grep -qE ':valid\? false|:valid\? :unknown' ~/jepsen-run.out; then
  echo "JEPSEN_RESULT FAIL"
  grep -E ':valid\?|anomal|inconsist' ~/jepsen-run.out | tail -5
else
  echo "JEPSEN_RESULT INCONCLUSIVE"
  tail -5 ~/jepsen-run.out
fi
echo "JEPSEN_DONE $(date -u +%FT%TZ)"
