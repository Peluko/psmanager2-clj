(ns notebooks.amz-transactions
  (:require
   [clj-http.client :as client]
   [clojure.edn :as edn]
   [tick.core :as t]
   [malli.core :as m]
   [malli.transform :as mt]
   [malli.error :as me]
   [malli.experimental.time :as met]
   [malli.registry :as mr]))

(mr/set-default-registry!
 (mr/composite-registry
  (m/default-schemas)
  (met/schemas)))

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

(def transaction-schema [:map
                         [:transactionType :string]])

(defn api-get [endpoint params]
  (let [url (str "https://sellingpartnerapi-eu.amazon.com" endpoint)
        user-agent "PSManager/0.6 (Language=Clojure/1.12/3/1577; Platform=Ubuntu/22.04)"
        now (t/format (t/formatter :iso-zoned-date-time) (t/zoned-date-time))]
    (try
      (->> (client/get url {:headers {:user-agent user-agent
                                      :x-amz-access-token bearer-token
                                      :x-amz-date now}
                            :query-params params
                            :accept :json
                            :as :json})
           :body)
      (catch Exception e
        e))))

(comment

  (def transactions (let [endpoint "/finances/2024-06-19/transactions"
                          query-params {:postedAfter "2025-11-01"
                                        :postedBefore "2025-11-08"}]
                      (->> (api-get endpoint query-params)
                           :payload
                           :transactions)))

  (let [descriptions (map #(:description %) transactions)]
    (distinct descriptions))

  (first transactions)

  (do
    (def transaction-schema (let [transaction-type-enum [:enum
                                                         "Shipment"
                                                         "ProductAdsPayment"
                                                         "ServiceFee"
                                                         "Refund"
                                                         "Transfer"]
                                  description-enum [:enum
                                                    "Order Payment"
                                                    "ProductAdsPayment"
                                                    "Subscription"
                                                    "FBACustomerReturn"
                                                    "Refund"
                                                    "FBAStorageBilling"
                                                    "FBARemoval"
                                                    "FBALongTermStorageBilling"
                                                    "Disbursement"
                                                    "FBAPostInboundTransportation"
                                                    "FBAShipmentInjectionFulfillmentFee"]
                                  related-identier-name-enum [:or :string [:enum ;; for reference only we list the relevant ones
                                                                           "ORDERID"
                                                                           "SHIPMENTID"
                                                                           "SETTLEMENTID"]]
                                  transaction-status-enum [:enum "RELEASED" "DEFERRED", "DEFERRED_RELEASED"]]
                              [:map
                               [:transactionType transaction-type-enum]
                               [:description description-enum]
                               [:relatedIdentifiers [:vector [:map [:relatedIdentifierName related-identier-name-enum] [:relatedIdentifierValue :string]]]]
                               [:transactionStatus transaction-status-enum]
                               [:postedDate [:time/instant {:pattern "yyyy-MM-ddThh:mm:ssZ"}]]]))

    (def transactions-schema [:vector transaction-schema])

    (try
      (-> transactions-schema
          (m/coerce transactions mt/strip-extra-keys-transformer))
      (catch Exception e
        (-> e ex-data :data :explain me/humanize))))

  :efc
  )