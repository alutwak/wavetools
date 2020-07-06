
(declaim (optimize (debug 3)))

(ql:quickload '(:dexador :plump :lquery :lparallel :str :cl-ppcre) :silent t)

(load (compile-file "wavespectrum.lisp"))

(defvar *station-cache* "~/.buoy/")
(defvar *hist-path* (concatenate 'string *station-cache* "~D/hist/"))

(defun parse-rtd (raw-spec raw-a1 raw-a2 raw-r1 raw-r2)
  "Parses real-time data formatted strings into a list of spectral-points. 

Each parameter comes in a separate file (which are provided to parse-rtd as strings). Each line of each file starts with
a 5-number date, and the values of the parameter for each frequency are given as: <val1> (<freq1>) <val2> (<freq2>) ...
The separation frequency is given in the spec file (which defines the 'c' parameter)."
  (let ((date-scan (ppcre:create-scanner "(\\d{4}) (\\d{2}) (\\d{2}) (\\d{2}) (\\d{2})"))  ; regex for extracting the date
        (fsep-scan (ppcre:create-scanner "([0-9.]+)"))                                     ; regex for getting the fsep
        (stat-freq-scan (ppcre:create-scanner "([0-9.]+) \\(([0-9.]+)\\)")))           ; regex for getting the stat and freq
    (labels ((split-by-lines (raw-str)
               "Splits into its individual lines"
               (str:split-omit-nulls #\Newline raw-str))
             (parse-row (row date? sep-freq?)
               "Parses a single row from one of the parameter files.

                If date? or sep-freq? are non-nil, they will be captured from the row"
               (let ((date
                      (if date? (ppcre:register-groups-bind ((#'parse-integer year month day hour min))
                                    (date-scan
                                     row)
                                  (encode-universal-time 0 min hour day month year))
                          nil))
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
                     point))))

      ;; Split each stat string by line
      ;; Parse all rows, skipping the header line
      (parse-all-rows
       (cdr (split-by-lines raw-spec))
       (cdr (split-by-lines raw-a1))
       (cdr (split-by-lines raw-a2))
       (cdr (split-by-lines raw-r1))
       (cdr (split-by-lines raw-r2))))))

(defun download-station-rtd (station-id)
  (flet ((get-data (path)
           (dex:get (format nil "https://www.ndbc.noaa.gov/data/realtime2/~D.~A" station-id path))))
    (let ((c-data (get-data "data_spec"))
          (a1-data (get-data "swdir"))
          (a2-data (get-data "swdir2"))
          (r1-data (get-data "swr1"))
          (r2-data (get-data "swr2")))
      `(,c-data ,a1-data ,a2-data ,r1-data ,r2-data))))
