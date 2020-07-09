
(declaim (optimize (debug 3)))

(ql:quickload '(:dexador :plump :lquery :lparallel :uiop :str :cl-ppcre :bt-semaphore) :silent t)

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

(defun this-year ()
  (nth-value 5 (get-decoded-time)))

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
  (c nil :type stream :read-only t)
  (a1 nil :type stream :read-only t)
  (a2 nil :type stream :read-only t)
  (r1 nil :type stream :read-only t)
  (r2 nil :type stream :read-only t))

(defun raw-data->list (rd)
  (list (raw-data-c rd) (raw-data-a1 rd) (raw-data-a2 rd) (raw-data-r1 rd) (raw-data-r2 rd)))

(defun raw-data-reset (rd)
  (mapc (lambda (getter)
          (file-position (funcall getter rd) 0))
        (list #'raw-data-c #'raw-data-a1 #'raw-data-a2 #'raw-data-r1 #'raw-data-r2)))

(defun raw-data-read-line (rd)
  "Reads lines from all raw data streams and returns them"
  (values-list
   (mapcar
    (lambda (strm)
      (read-line strm))
    (raw-data->list rd))))

(defun eof-p (stream)
  (not (peek-char nil stream nil nil)))

(defun raw-data-eof-p (rd)
  (every #'eof-p (raw-data->list rd)))

(defvar *date-scan* (ppcre:create-scanner "(\\d{4}) (\\d{2}) (\\d{2}) (\\d{2}) (\\d{2})")
  "Regex for retrieving the date from a buoy data row")

(defun parse-date (row-str)
  (ppcre:register-groups-bind
   ((#'parse-integer year month day hour min))
   (*date-scan*
    row-str)
   (encode-universal-time 0 min hour day month year)))

;;; Real Time Data

(defun parse-rtd (rd)
  "Parses real-time data formatted strings into a list of spectral-points. 

Each parameter comes in a separate file (which are provided to parse-rtd as strings). Each line of each file starts with
a 5-number date, and the values of the parameter for each frequency are given as: <val1> (<freq1>) <val2> (<freq2>) ...
The separation frequency is given in the spec file (which defines the 'c' parameter)."
  (let ((fsep-scan (ppcre:create-scanner "([0-9.]+)"))                                     ; regex for getting the fsep
        (stat-freq-scan (ppcre:create-scanner "([0-9.]+) \\(([0-9.]+)\\)")))           ; regex for getting the stat and freq
    (labels ((parse-line (line c-data?)
               "Parses a single line from one of the parameter files.

                If c-data? is non-nil, the freqs and fsep values will be captured"
               (let ((date (if c-data? (parse-date line) nil))
                     (fsep (if c-data?
                               (ppcre:register-groups-bind ((#'read-from-string fsep))
                                   (fsep-scan
                                    line
                                    :start 17)
                                 fsep)
                               nil))
                     (data)
                     (freqs))
                 (ppcre:do-register-groups ((#'read-from-string stat freq))
                     (stat-freq-scan
                      line
                      nil
                      :start (if c-data? 23 17))
                   (progn
                     (setq data (cons stat data))
                     (setq freqs (cons freq freqs))))
                 (list (reverse data) (reverse freqs) date fsep)))

             (parse-all-lines (c-line a1-line a2-line r1-line r2-line)
               "Parses lines for all raw data streams"
               (let* ((c-f-d-fsep (parse-line c-line t))
                      (a1 (car (parse-line a1-line nil)))
                      (a2 (car (parse-line a2-line nil)))
                      (r1 (car (parse-line r1-line nil)))
                      (r2 (car (parse-line r2-line nil))))
                 (make-spectral-point
                  (nth 2 c-f-d-fsep)               ; time stamp
                  (map 'vector #'make-spect-params ; params
                       (nth 1 c-f-d-fsep)          ; f
                       (car c-f-d-fsep)            ; c
                       a1                      
                       a2
                       r1
                       r2)
                  (nth 3 c-f-d-fsep)))) ; fsep

             (parse-next-entry ()
               "Recursively reads lines from the raw data streams and parses them to create spectral points"
               (if (raw-data-eof-p rd)
                   nil
                   (cons
                    (multiple-value-bind
                     (c a1 a2 r1 r2)
                     (raw-data-read-line rd)
                     (parse-all-lines c a1 a2 r1 r2))
                    (parse-next-entry)))))

      (raw-data-read-line rd) ;; Discard header
      (reverse (parse-next-entry)))))

(defun cache-station-rtd (station-id raw-c raw-a1 raw-a2 raw-r1 raw-r2)
  "Saves data to the real-time data cache for the station with station-id."
  (let ((rtd-data-path (ensure-rtd-data-path-exists station-id)))
    (mapcar (lambda (data name)
              (str:to-file (str:concat rtd-data-path name) data))
            (list raw-c raw-a1 raw-a2 raw-r1 raw-r2)
            '("c" "a1" "a2" "r1" "r2"))))

(defun download-station-rtd (station-id &optional cache-data)
  "Downloads the real-time data for the specified station ID"
  (flet ((get-data (path)
           (dex:get (format nil "https://www.ndbc.noaa.gov/data/realtime2/~D.~A" station-id path))))
    (let ((c-data (get-data "data_spec"))
          (a1-data (get-data "swdir"))
          (a2-data (get-data "swdir2"))
          (r1-data (get-data "swr1"))
          (r2-data (get-data "swr2")))

      (when cache-data
        ;; Do it in a thread so we don't hang this up
        (bt:make-thread
         (lambda ()
           (cache-station-rtd station-id c-data a1-data a2-data r1-data r2-data))))

      (parse-rtd (make-raw-data
                  (make-string-input-stream c-data)
                  (make-string-input-stream a1-data)
                  (make-string-input-stream a2-data)
                  (make-string-input-stream r1-data)
                  (make-string-input-stream r2-data))))))

(defun read-station-rtd-cache (station-id)
  (let* ((cache-path (get-rtd-data-path station-id))
         (rd (make-raw-data
              (open (str:concat cache-path "c"))
              (open (str:concat cache-path "a1"))
              (open (str:concat cache-path "a2"))
              (open (str:concat cache-path "r1"))
              (open (str:concat cache-path "r2")))))
    (prog1
        (parse-rtd rd)
      (mapc #'close (raw-data->list rd)))))

(defun get-rtd-data-date (raw-data)
  (handler-case
      (with-open-file (f (str:concat (get-rtd-data-path station-id) "c"))
        (read-line f)
        (let ((line1 (read-line f)))
          (parse-date line1)))
    (file-does-not-exist () nil)))

(defun get-rtd-cache-date (station-id)
  (handler-case
      (with-open-file (f (str:concat (get-rtd-data-path station-id) "c"))
        (read-line f)
        (let ((line1 (read-line f)))
          (parse-date line1)))
    (file-does-not-exist () nil)))

;;; Historical Data

(defun parse-hist (rd)
  "Parses historical data formatted strings into a list of spectral-points. 

Each parameter comes in a separate file (which are provided to parse-rtd as strings). Each line of each file starts with
a 5-number date, and the values of the parameter for each frequency are given as: <val1> <val2> ...
The separation frequency is given in the spec file (which defines the 'c' parameter)."
  (let ((stat-freq-scan (ppcre:create-scanner "\\s+([0-9.]+)")) ; regex for getting the stat and freq
        (freqs))                                                ; register for frquencies 
    (labels ((parse-line (line c-data?)
               "Parses a single line from one of the parameter files.

                If c-data? is non-nil, the date will be captured"
               (let ((date (if c-data? (parse-date line) nil))
                     (data))
                 (ppcre:do-register-groups ((#'read-from-string stat))
                     (stat-freq-scan
                      line
                      nil
                      :start 16)
                   (setq data (cons (float stat) data)))
                 (list (reverse data) date)))
             
             (parse-all-lines (c-line a1-line a2-line r1-line r2-line)
               "Parses lines for all raw data streams"
               (let* ((c-d (parse-line c-line t))
                      (a1 (car (parse-line a1-line nil)))
                      (a2 (car (parse-line a2-line nil)))
                      (r1 (car (parse-line r1-line nil)))
                      (r2 (car (parse-line r2-line nil))))
                 (make-spectral-point
                  (cadr c-d)                       ; time stamp
                  (map 'vector #'make-spect-params ; params
                       freqs
                       (car c-d)        ; c
                       a1                      
                       a2
                       r1
                       r2))))

             (parse-next-entry ()
               "Recursively reads lines from the raw data streams and parses them to create spectral points"
               (if (raw-data-eof-p rd)
                   nil
                   (cons
                    (multiple-value-bind
                          (c a1 a2 r1 r2)
                        (raw-data-read-line rd)
                      (parse-all-lines c a1 a2 r1 r2))
                    (parse-next-entry)))))

      ;; Get the frequencies
      (ppcre:do-register-groups ((#'read-from-string freq))
          (stat-freq-scan
           (raw-data-read-line rd)      ; Returns first line of c data and throws away the other data files' lines
           nil
           :start 16)
        (setq freqs (cons freq freqs)))

      ;; Reverse them to put them in order      
      (setq freqs (reverse freqs))

      ;; Return parsed data
      (reverse (parse-next-entry)))))


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

      (parse-hist (make-raw-data
                   (make-string-input-stream c-data)
                   (make-string-input-stream a1-data)
                   (make-string-input-stream a2-data)
                   (make-string-input-stream r1-data)
                   (make-string-input-stream r2-data))))))

