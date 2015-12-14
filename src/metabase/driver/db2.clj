(ns metabase.driver.db2
  (:require (clojure [set :as set]
                     [string :as s])
            (korma [core :as k]
                   [db :as kdb])
            [korma.sql.utils :as kutils]
            [metabase.driver :as driver]
            [metabase.driver.generic-sql :as sql]
            [metabase.util.korma-extensions :as kx])) ; need to import this in order to load JDBC driver

(Class/forName "com.ibm.db2.jcc.DB2Driver")

(defn- column->base-type
  ""
  [_ column-type]
  ({:BIGINT     :BigIntegerField
    :BINARY     :UnknownField
    :BIT        :UnknownField
    :BLOB       :UnknownField
    :CHAR       :CharField
    :DATE       :DateField
    :DATETIME   :DateTimeField
    :DECFLOAT   :DecimalField
    :DECIMAL    :DecimalField
    :DOUBLE     :FloatField
    :ENUM       :UnknownField
    :FLOAT      :FloatField
    :GRAPHIC    :UnknownField
    :INT        :IntegerField
    :INTEGER    :IntegerField
    :LONGBLOB   :UnknownField
    :LONGTEXT   :TextField
    :MEDIUMBLOB :UnknownField
    :MEDIUMINT  :IntegerField
    :MEDIUMTEXT :TextField
    :NUMERIC    :DecimalField
    :REAL       :FloatField
    :SET        :UnknownField
    :TEXT       :TextField
    :TIME       :TimeField
    :TIMESTAMP  :DateTimeField
    :VARBINARY  :UnknownField
    :VARCHAR    :TextField} column-type)) ; auto-incrementing integer (ie pk) field

(defn db2
  "Create a database specification for a db2 database. Opts should include keys
  for :user and :password. You can also optionally set host and port."
  [{:keys [host port db]
    :or {host "localhost", port 50000, db ""}
    :as opts}]
  (merge {:classname "com.ibm.db2.jcc.DB2Driver"
          :subprotocol "db2"
          :subname (str "//" host ":" port "/" db)}
         (dissoc opts :host :port :db)))

(defn- connection-details->spec [_ details]
  (-> details
      (set/rename-keys {:dbname :db})
      db2))

(defn- date-part [unit expr]
  (k/sqlfn :DATEPART (k/raw (name unit)) expr))

(defn- date-add [unit & exprs]
  (apply k/sqlfn* :DATEADD (k/raw (name unit)) exprs))

(defn- date
  [_ unit expr]
  (case unit
    :default         (kx/->datetime expr)
    :minute          (kx/cast :SMALLDATETIME expr)
    :minute-of-hour  (date-part :minute expr)
    :hour            (kx/->datetime (kx/format "yyyy-MM-dd HH:00:00" expr))
    :hour-of-day     (date-part :hour expr)
    :day             (kx/->datetime (kx/->date expr))
    :day-of-week     (date-part :weekday expr)
    :day-of-month    (date-part :day expr)
    :day-of-year     (date-part :dayofyear expr)
    ;; Subtract the number of days needed to bring us to the first day of the week, then convert to date
    ;; The equivalent SQL looks like:
    ;;     CAST(DATEADD(day, 1 - DATEPART(weekday, %s), CAST(%s AS DATE)) AS DATETIME)
    :week            (kx/->datetime
                      (date-add :day
                                (kx/- 1 (date-part :weekday expr))
                                (kx/->date expr)))
    :week-of-year    (date-part :iso_week expr)
    :month           (kx/->datetime (kx/format "yyyy-MM-01" expr))
    :month-of-year   (date-part :month expr)
    ;; Format date as yyyy-01-01 then add the appropriate number of quarter
    ;; Equivalent SQL:
    ;;     DATEADD(quarter, DATEPART(quarter, %s) - 1, FORMAT(%s, 'yyyy-01-01'))
    :quarter         (date-add :quarter
                               (kx/dec (date-part :quarter expr))
                               (kx/format "yyyy-01-01" expr))
    :quarter-of-year (date-part :quarter expr)
    :year            (date-part :year expr)))

(defn- date-interval [_ unit amount]
  (date-add unit amount (k/sqlfn :GETUTCDATE)))

(defn- unix-timestamp->timestamp [_ expr seconds-or-milliseconds]
  (case seconds-or-milliseconds
    ;; The second argument to DATEADD() gets casted to a 32-bit integer. BIGINT is 64 bites, so we tend to run into
    ;; integer overflow errors (especially for millisecond timestamps).
    ;; Work around this by converting the timestamps to minutes instead before calling DATEADD().
    :seconds      (date-add :minute (kx// expr 60) (kx/literal "1970-01-01"))
    :milliseconds (recur nil (kx// expr 1000) :seconds)))

(defn- apply-limit [_ korma-query {value :limit}]
  (k/modifier korma-query (format "FETCH FIRST %d ROWS ONLY" value)))

(defrecord DB2Driver []
  clojure.lang.Named
  (getName [_] "IBM DB2"))

(extend DB2Driver
  driver/IDriver
  (merge (sql/IDriverSQLDefaultsMixin)
         {:date-interval  date-interval
          :details-fields (constantly [{:name         "host"
                                        :display-name "Host"
                                        :default      "localhost"}
                                       {:name         "port"
                                        :display-name "Port"
                                        :type         :integer
                                        :default      50000}
                                       {:name         "db"
                                        :display-name "Database name"
                                        :placeholder  "BirdsOfTheWorld"
                                        :required     true}
                                       {:name         "instance"
                                        :display-name "Database instance name"
                                        :placeholder  "N/A"}
                                       {:name         "user"
                                        :display-name "Database username"
                                        :placeholder  "What username do you use to login to the database?"
                                        :required     true}
                                       {:name         "password"
                                        :display-name "Database password"
                                        :type         :password
                                        :placeholder  "*******"}])})

  sql/ISQLDriver
  (merge (sql/ISQLDriverDefaultsMixin)
         {:apply-limit               apply-limit
          :column->base-type         column->base-type
          :connection-details->spec  connection-details->spec
          :current-datetime-fn       (constantly (k/sqlfn* :GETUTCDATE))
          :date                      date
          :excluded-schemas          (constantly #{"sys" "INFORMATION_SCHEMA"})
          :stddev-fn                 (constantly :STDEV)
          :string-length-fn          (constantly :LEN)
          :unix-timestamp->timestamp unix-timestamp->timestamp}))

(driver/register-driver! :db2 (DB2Driver.))
