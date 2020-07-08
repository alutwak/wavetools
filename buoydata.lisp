
(declaim (optimize (debug 3)))

(ql:quickload '(:dexador :plump :lquery :lparallel :uiop :str :cl-ppcre) :silent t)

;;(load (compile-file "wavespectrum.lisp"))

;;; Utilities

;;; Path definitions and retrieval functions

(defvar *buoy-cache* (uiop:native-namestring "~/.buoy/")
  "Main path for the bouy data cache")

(defun ensure-cache-exists ()
  "Ensures that the cache directory exists."
  (ensure-directories-exist *buoy-cache*))

(defvar *station-cache* (concatenate 'string *buoy-cache* "~D/")
  "Path for caching data for a specific station. The ~D is the station ID.")

(defun get-station-cache-path (station-id)
  (format nil *station-cache* station-id))

(defun ensure-station-cache-exists (station-id)
  "Ensures that the cache directory exists for the station and returns the path."
  (let ((cache-path (get-station-cache-path station-id)))
    (ensure-directories-exist cache-path)
    cache-path))

(defvar *rtd-path* "rtd/"
  "Path for real-time data cache. This will contain data for the last 45 days. Some of that may overlap with historical data.
In general, the historical data will be better than the real-time data. This data gets updated at least daily, so this cache
is only really useful when multiple hits on the data are done within a few hours.")

(defun get-rtd-data-path (station-id)
  (concatenate 'string (get-station-cache-path station-id) *rtd-path*))

(defun ensure-rtd-data-path-exists (station-id)
  (let ((cache-path (get-rtd-data-path station-id)))
    (ensure-directories-exist cache-path)
    cache-path))

(defvar *hist-path* "hist/"
  "Path for historical data cache.")

(defun get-hist-data-path (station-id &optional month-or-year)
  (str:concat (get-station-cache-path station-id) *hist-path* month-or-year))

(defun ensure-hist-path-exists (station-id &optional month-or-year)
  (let ((cache-path (get-hist-data-path station-id month-or-year)))
    (ensure-directories-exist cache-path)
    cache-path))

;; ---------------------------------- Station metadata ------------------------------------

(defstruct (station-metadata (:constructor make-station-metadata (lat lon water-depth)))
  (lat 0.0 :type float :read-only t)
  (lon 0.0 :type float :read-only t)
  (water-depth 0.0 :type float :read-only t))

(defun download-station-metadata (station-id)
  (let* ((loc-scan (ppcre:create-scanner "\\s+([0-9.]+) (N|S) ([0-9.]+) (E|W)"))
         (depth-scan (ppcre:create-scanner "\\s+Water depth: ([0-9.]+) m"))
         (request (dex:get (format nil "https://www.ndbc.noaa.gov/station_page.php?station=~D&uom=E&tz=STN" station-id)))
         (parsed-req (lquery:$ (lquery:initialize request)))
         (metadata-str (aref (lquery:$ parsed-req "#stn_metadata p" (text)) 0)) ; This comes as an annoying paragraph
         (lat)
         (lon)
         (water-depth))
    (dolist (line (str:lines metadata-str) (make-station-metadata lat lon water-depth))
      (let* ((lat-lon? (ppcre:register-groups-bind ((#'read-from-string lat ltdir lon lndir)) 
                           (loc-scan line)
                         (cons (* lat (if (string= ltdir "S") -1 1)) (* lon (if (string= lndir "E") -1 1)))))
             (w-d? (ppcre:register-groups-bind ((#'read-from-string depth))
                       (depth-scan line)
                     (float depth))))
        (when lat-lon?
          (setq lat (car lat-lon?) lon (cdr lat-lon?)))
        (when w-d? (setq water-depth w-d?))))))

;; ----------------------------------- Raw Data (in string format) -------------------------------

(defstruct (raw-data (:constructor make-raw-data (c a1 a2 r1 r2)))
  (c nil :read-only t)
  (a1 nil :read-only t)
  (a2 nil :read-only t)
  (r1 nil :read-only t)
  (r2 nil :read-only t))

(defvar *date-scan* (ppcre:create-scanner "(\\d{4}) (\\d{2}) (\\d{2}) (\\d{2}) (\\d{2})")
  "Regex for retrieving the date from a buoy data row")



(defun parse-date (row-str)
  (ppcre:register-groups-bind
   ((#'parse-integer year month day hour min))
   (*date-scan*
    row-str)
   (encode-universal-time 0 min hour day month year)))


;;; Real Time Data

(defun parse-rtd (raw)
  "Parses real-time data formatted strings into a list of spectral-points. 

Each parameter comes in a separate file (which are provided to parse-rtd as strings). Each line of each file starts with
a 5-number date, and the values of the parameter for each frequency are given as: <val1> (<freq1>) <val2> (<freq2>) ...
The separation frequency is given in the spec file (which defines the 'c' parameter)."
  (let ((fsep-scan (ppcre:create-scanner "([0-9.]+)"))                                     ; regex for getting the fsep
        (stat-freq-scan (ppcre:create-scanner "([0-9.]+) \\(([0-9.]+)\\)")))           ; regex for getting the stat and freq
    (labels ((parse-row (row date? sep-freq?)
               "Parses a single row from one of the parameter files.

                If date? or sep-freq? are non-nil, they will be captured from the row"
               (let ((date (if date? (parse-date row) nil))
                     (fsep (if sep-freq?
                               (ppcre:register-groups-bind ((#'read-from-string fsep))
                                   (fsep-scan
                                    row
                                    :start 17)
                                 fsep)
                               nil))
                     (data)
                     (freqs))
                 (ppcre:do-register-groups ((#'read-from-string stat freq))
                     (stat-freq-scan
                      row
                      nil
                      :start (if sep-freq? 23 17))
                   (progn
                     (setq data (cons stat data))
                     (setq freqs (cons freq freqs))))
                 (list (reverse data) (reverse freqs) date fsep)))
             (parse-all-rows (l-spec l-a1 l-a2 l-r1 l-r2)
               "Recursively parses all rows (after they have been split into lines). This assumes that the header has been
                skipped already."
               (let* ((c-f-d-fsep (parse-row (car l-spec) t t))
                      (a1 (car (parse-row (car l-a1) nil nil)))
                      (a2 (car (parse-row (car l-a2) nil nil)))
                      (r1 (car (parse-row (car l-r1) nil nil)))
                      (r2 (car (parse-row (car l-r2) nil nil)))
                      (point (make-spectral-point
                              (nth 2 c-f-d-fsep)                   ; time stamp
                              (map 'vector #'make-spect-params     ; params
                                   (nth 1 c-f-d-fsep)      ; f
                                   (car c-f-d-fsep)        ; c
                                   a1                      
                                   a2
                                   r1
                                   r2)
                              (nth 3 c-f-d-fsep))))                ; fsep
                 (if (every #'cdr (list l-spec l-a1 l-a2 l-r1 l-r2))
                     (cons point (parse-all-rows (cdr l-spec) (cdr l-a1) (cdr l-a2) (cdr l-r1) (cdr l-r2)))
                     `(,point)))))

      ;; Split each stat string by line
      ;; Parse all rows, skipping the header line
      (reverse (parse-all-rows
                (cdr (str:lines (raw-data-c raw)))
                (cdr (str:lines (raw-data-a1 raw)))
                (cdr (str:lines (raw-data-a2 raw)))
                (cdr (str:lines (raw-data-r1 raw)))
                (cdr (str:lines (raw-data-r2 raw))))))))

(defun download-station-rtd (station-id)
  "Downloads the real-time data for the specified station ID"
  (flet ((get-data (path)
           (dex:get (format nil "https://www.ndbc.noaa.gov/data/realtime2/~D.~A" station-id path))))
    (let ((c-data (get-data "data_spec"))
          (a1-data (get-data "swdir"))
          (a2-data (get-data "swdir2"))
          (r1-data (get-data "swr1"))
          (r2-data (get-data "swr2")))
      (make-raw-data c-data a1-data a2-data r1-data r2-data))))

(defun cache-station-rtd (station-id raw-data)
  "Saves data to the real-time data cache for the station with station-id."
  (let ((rtd-data-path (ensure-rtd-data-path-exists station-id)))
    (mapcar (lambda (getter name)
              (str:to-file (str:concat rtd-data-path name) (funcall getter raw-data)))
            (list #'raw-data-c #'raw-data-a1 #'raw-data-a2 #'raw-data-r1 #'raw-data-r2)
            '("c" "a1" "a2" "r1" "r2"))))

(defun read-rtd-cache (station-id)
  (let* ((cache-path (get-rtd-data-path station-id)) 
         (c-data (str:from-file (str:concat cache-path "c")))
         (a1-data (str:from-file (str:concat cache-path "a1")))
         (a2-data (str:from-file (str:concat cache-path "a2")))
         (r1-data (str:from-file (str:concat cache-path "r1")))
         (r2-data (str:from-file (str:concat cache-path "r2"))))
    (make-raw-data c-data a1-data a2-data r1-data r2-data)))

(defun get-rtd-cache-date (station-id)
  (handler-case
      (with-open-file (f (str:concat (get-rtd-data-path station-id) "c"))
        (read-line f)
        (let ((line1 (read-line f)))
          (parse-date line1)))
    (file-does-not-exist () nil)))

;;; Historical Data

(defun this-year ()
  (nth-value 5 (get-decoded-time)))

(defun download-station-hist (station-id year-or-month)
  (labels ((get-year-data (char-key path)
             (dex:get
              (format
               nil
               "https://www.ndbc.noaa.gov/view_text_file.php?filename=~D~C~D.txt.gz&dir=data/historical/~A/"
               station-id
               char-key
               year-or-month
               path)))
           (get-month-data (path)
             (dex:get
              (format
               nil
               "https://www.ndbc.noaa.gov/data/~A/~A/~D.txt"
               path
               year-or-month
               station-id)))
           (get-data (char-key path)
             (if (numberp year-or-month)
                 (get-year-data char-key path)
                 (get-month-data path))))
    (let ((c-data (get-data #\w "swden"))
          (a1-data (get-data #\d "swdir"))
          (a2-data (get-data #\i "swdir2"))
          (r1-data (get-data #\j "swr1"))
          (r2-data (get-data #\k "swr2")))
      (make-raw-data c-data a1-data a2-data r1-data r2-data))))
