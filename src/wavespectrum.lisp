(defpackage :wavetools/wavespectrum
  (:use :cl)
  (:nicknames :wavespect)
  (:import-from :lisp-binary
                :defbinary
                :counted-array)
  (:import-from :array-operations
                :linspace
                :generate)
  (:export

   :*python-time-offset*
   :spectral-point
   :spectral-point-ts
   :spectral-point-c
   :spectral-point-a1
   :spectral-point-a2
   :spectral-point-r1
   :spectral-point-r2
   :spectral-point-fsep
   :make-spect-point

   :spreading-function
   :create-direction-space
   :directional-spectrum
   :spectrum
   :dump-spectrum))

(in-package :wavetools/wavespectrum)

(defvar *python-time-offset* (encode-universal-time 0 0 0 1 1 1970 0))

;;Defines the spectral data for a single reading from a buoy
(lisp-binary:defbinary
    spectral-point ()
    (ts 0 :type 32)                                    ; timestamp       
    (c #() :type (lisp-binary:counted-array 1 float))  ; non-directional spectral density
    (a1 #() :type (lisp-binary:counted-array 1 float)) ; mean wave direction
    (a2 #() :type (lisp-binary:counted-array 1 float)) ; principal wave direction
    (r1 #() :type (lisp-binary:counted-array 1 float))
    (r2 #() :type (lisp-binary:counted-array 1 float))
    (fsep 9.999 :type float)) ; separation frequency

(defun make-spect-point (ts c a1 a2 r1 r2 &optional (fsep 9.999))
  (make-spectral-point :ts ts :c c :a1 a1 :a2 a2 :r1 r1 :r2 r2 :fsep fsep))

(defun spreading-function (theta a1 a2 r1 r2)
  (let ((alpha (- (* 1.5 pi) theta)))
    (/
     (+ 0.5
        (* r1 (cos (- alpha a1)))
        (* r2 (cos (* 2 (- alpha a2)))))
     pi)))

(defun directional-spectrum (theta c a1 a2 r1 r2)
  (* c (spreading-function theta a1 a2 r1 r2)))

(defun create-direction-space (dir-res &optional (endpoint t))
  (let ((stop (if endpoint
                  (* 2 pi)
                  (* (/ (- dir-res 1) dir-res) 2 pi))))
    (aops:linspace
     0
     stop
     dir-res)))

(defun spectrum (spoint freqs dir-res)
  (let* (
         ;; Linear radial angle space
         (theta (create-direction-space dir-res))

         ;; Calculates a 2-D mesh of S(theta, freq)
         (spect
           (aops:generate
            (lambda (subscr)
              (let ((pref (cadr subscr))
                    (tref (car subscr)))
                (directional-spectrum
                 (aref theta tref)      ; theta
                 (aref (spectral-point-c spoint) pref)
                 (aref (spectral-point-a1 spoint) pref)
                 (aref (spectral-point-a2 spoint) pref)
                 (aref (spectral-point-r1 spoint) pref)
                 (aref (spectral-point-r2 spoint) pref))))
            `(,(length theta) ,(length freqs)) ; size of array
            :subscripts)))
    (values spect theta freqs)))


(defun dump-spectrum (spoint freqs dir-res &optional stream dump-f-and-d)
  (let ((binary-out (or
                     stream
                     (sb-ext::make-fd-stream
                      1
                      :name "binary-output"
                      :output t
                      :buffering :none
                      :element-type '(unsigned-byte 8)))))
    (lisp-binary:write-binary-type (- (spectral-point-ts spoint) *python-time-offset*) 32 binary-out)
    (lisp-binary:write-binary-type 0.0 'float binary-out)  ;; depth
    (lisp-binary:write-binary-type 0.0 'float binary-out)  ;; wind speed
    (lisp-binary:write-binary-type 0.0 'float binary-out)  ;; wind dir
    (lisp-binary:write-binary-type 0.0 'float binary-out)  ;; current speed
    (lisp-binary:write-binary-type 0.0 'float binary-out)  ;; current dir
    (multiple-value-bind (spect theta freqs)
        (spectrum spoint freqs dir-res)
      (when dump-f-and-d
        (lisp-binary:write-binary-type freqs '(lisp-binary:counted-array 2 float) binary-out)
        (lisp-binary:write-binary-type theta '(lisp-binary:counted-array 2 float) binary-out))
      (lisp-binary:write-binary-type (aops:flatten spect) '(lisp-binary:counted-array 4 float) binary-out))))
