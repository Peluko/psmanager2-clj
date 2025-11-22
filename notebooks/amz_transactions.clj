;; # AMZ Transactions

(ns notebooks.amz-transactions
  {:nextjournal.clerk/visibility {:code :hide :result :hide}}
  (:require
   [clj-http.client :as client]
   [tick.core :as t]
   [malli.core :as m]
   [malli.transform :as mt]
   [malli.error :as me]
   [malli.experimental.time :as met]
   [malli.registry :as mr]
   [clojure.set]
   [repl]))

(mr/set-default-registry!
 (mr/composite-registry
  (m/default-schemas)
  (met/schemas)))

(def amz-mkts-id {:ie "A28R8C7NBKEWEA" ;; Ireland
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

(def amz-ids-mkt (clojure.set/map-invert amz-mkts-id))

(def bearer-token (let [{:biff/keys [secret]
                         :psmanager.amazon/keys [auth-endpoint]} (repl/get-context)
                        refresh-token (secret :psmanager.amazon/refresh-token)
                        client-id (secret :psmanager.amazon/client-id)
                        secret (secret :psmanager.amazon/secret)]
                    (->
                     (client/post auth-endpoint {:form-params {:grant_type "refresh_token"
                                                               :refresh_token refresh-token
                                                               :client_id client-id
                                                               :client_secret secret}
                                                 :as :json})
                     :body
                     :access_token)))

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

{:nextjournal.clerk/visibility {:code :fold :result :show}}

;; ## Transaction schema
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
                              transaction-status-enum [:enum "RELEASED" "DEFERRED", "DEFERRED_RELEASED"]
                              currency-code-enum [:enum "EUR" "PLN" "SEK" "GBP"]
                              marketplace-id-enum [:or :string (into [:enum] (vals amz-mkts-id))]]
                          [:map
                           [:transactionType transaction-type-enum]
                           [:description description-enum]
                           [:relatedIdentifiers [:vector [:map [:relatedIdentifierName related-identier-name-enum] [:relatedIdentifierValue :string]]]]
                           [:transactionStatus transaction-status-enum]
                           [:postedDate inst?]
                           [:totalAmount [:map [:currencyAmount number?] [:currencyCode currency-code-enum]]]
                           [:marketplaceDetails [:map [:marketplaceId marketplace-id-enum] [:marketplaceName :string]]]]))

(def transactions-schema [:vector transaction-schema])

;; ### Campos útiles
;; - TransactionType: [ Shipment, ProductAdsPayment, ServiceFee, Refund, Transfer ]
;; - Description: [ Order Payment, ProductAdsPayment, Subscription, FBACustomerReturn, Refund, FBAStorageBilling, FBARemoval, FBALongTermStorageBilling, Disbursement, FBAShipmentInjectionFulfillmentFee ]
;; - RelatedIdentifiers: ORDERID, SHIPMENTID, SETTLEMENTID
;; - TransactionStatus: RELEASED, DEFERRED, DEFERRED_RELEASED
;; - PostedDate: fecha efectiva de la transacción si el estado es RELEASED. Si es DEFERRED, el pago está retenido. Cuando se libere será DEFERRED_RELEASED.
;; - TotalAmount:
;; - MarketplaceDetails:
;; - Contexts: cuando el pago es retenido, aquí aparece un item con el campo DeferralReason a DD7 y MaturityDate con la fecha de liberación (los que no están liberados aparecen con fecha futura)
;; - Breakdowns: desglose, suele tener dos ítems
;;   - Cuando es Shipment u otra cosa
;;     - Sales, a veces a 0 si no es venta
;;       - ProductCharges
;;       - Tax
;;     - Expenses
;;       - AmazonFees, ServicesFees...
;;   - Cuando es Refund
;;     - Refund Sales
;;     - RefundExpenses

;; ## Test transactions
(def transactions (let [endpoint "/finances/2024-06-19/transactions"
                        query-params {:postedAfter "2025-11-01"
                                      :postedBefore "2025-11-08"}]
                    (->> (api-get endpoint query-params)
                         :payload
                         :transactions)))

(let [descriptions (map #(:description %) transactions)]
  (distinct descriptions))

(first transactions)

;; ## Process

^{:nextjournal.clerk/auto-expand-results? true}
(try
  (-> transaction-schema
      (m/coerce (first transactions) (mt/transformer mt/json-transformer mt/strip-extra-keys-transformer)))
  (catch Exception e
    (-> e ex-data :data :explain me/humanize)))

;; ## Validate

(let [coerced (m/coerce transactions-schema transactions (mt/transformer mt/json-transformer mt/strip-extra-keys-transformer))]
  (-> transaction-schema
      (m/explain (first coerced))
      (me/humanize)))

^{:nextjournal.clerk/visibility {:code :hide :result :hide}}
(comment

  :efc)