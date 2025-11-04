(ns scratch.amz-transactions
  (:require
   [clj-http.client :as client]
   [clojure.edn :as edn]
   [tick.core :as t]
   [clj-reload.core]))

(def amz-mkts {:ie "A28R8C7NBKEWEA" ;; Ireland
               :es "A1RKKUPIHCS9HS" ;; Spain
               :uk "A1F83G8C2ARO7P" ;; United Kingdom
               :fr "A13V1IB3VIYZZH" ;; France
               :be "AMEN7PMS3EDWL" ;; Belgium
               :nl "A1805IZSGTT6HS" ;; Netherlands
               :de "A1PA6795UKMFR9" ;; Germany
               :it "APJ6JRA9NG5V4" ;; Italy
               :se "A2NODRKZP88ZB9" ;; Sweden
               :za "AE08WJ6YKNBMC" ;; South Africa
               :pl "A1C3SOZRARQ6R3" ;; Poland
               :eg "ARBP9OOSHTCHU" ;; Egypt
               :tr "A33AVAJ2PDY3EV" ;; Turkey
               :sa "A17E79C6D8DWNP" ;; Saudi Arabia
               :ae "A2VIGQ35RCS4UG" ;; United Arab Emirates
               :in "A21TJRUUN4KGV" ;; India
               })

(def config (edn/read-string (slurp ".config.edn")))

(def bearer-token (let [amz-config (:amz config)
                        server "https://api.amazon.com"
                        path "/auth/o2/token"
                        url (str server path)]
                    (->
                     (client/post url {:form-params {:grant_type "refresh_token"
                                                     :refresh_token (:refresh-token amz-config)
                                                     :client_id (:client-id amz-config)
                                                     :client_secret (:client-secret amz-config)}
                                       :as :json})
                     :body
                     :access_token)))

(defn api-get [endpoint]
  (let [url (str "https://sellingpartnerapi-eu.amazon.com" endpoint)
        user-agent "PSManager/0.6 (Language=Clojure/1.12/3/1577; Platform=Ubuntu/22.04)"
        now (t/format (t/formatter :iso-zoned-date-time) (t/zoned-date-time))]
    (try
      (client/get url {:headers {:user-agent user-agent
                                 :x-amz-access-token bearer-token
                                 :x-amz-date "20251103T183101"}})
      (catch Exception e
        e))))

(comment

  (clj-reload.core/reload)

  (let [endpoint (str "/catalog/2022-04-01/items/B0FRY77BJN" #_"?marketplaceIds=" #_(:es amz-mkts))]
    (api-get endpoint))


  :efc
  )