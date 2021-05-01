(setf *compile-verbose* nil)

(let ((quicklisp-init (merge-pathnames "quicklisp/setup.lisp"
                                       (user-homedir-pathname))))
  (when (probe-file quicklisp-init)
    (load quicklisp-init)))

(defvar *this-dir* (directory-namestring *load-pathname*))

(let ((this-dir (directory-namestring *load-pathname*)))
  (setq asdf:*central-registry* (cons this-dir asdf:*central-registry*)))

(handler-bind ((warning
                 #'(lambda (c)
                     (invoke-restart 'muffle-warning))))
  (asdf:load-system :wavetools :silent t))

(ql:quickload :unix-opts :silent t)

(in-package :wavetools)

(defun parse-date-time (arg)
  (ppcre:register-groups-bind
      ((#'read-from-string month day year hour min))
      ("([0-9]+)-([0-9]+)-([0-9]+) ([0-9]+):([0-9]+)" arg)
    (encode-universal-time 0 min hour day month year)))

(opts:define-opts
    (:name :help
     :description "Print this help message"
     :short #\h
     :long "help")
    (:name :station
     :description "The station name to use"
     :short #\s
     :long "station"
     :required t
     :arg-parser #'identity)
  (:name :date-time
   :description "The date and time with format \"MM-DD-YY HH:MM\""
   :short #\d
   :long "date-time"
   :required t
   :arg-parser #'parse-date-time))

(defvar *args* (opts:get-opts))

(when (getf *args* :help)
  (opts:describe)
  (quit))

(let* ((station-id (getf *args* :station))
       (time (getf *args* :date-time))
       (station (get-station-at-time station-id time t)))
  (dump-station station 360))

