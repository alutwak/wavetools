;;; (ql:quickload '(:dexador :plump :lquery :lparallel :uiop :str :cl-ppcre :bt-semaphore :array-operations :lisp-binary))

;; (defpackage :wavetools/wavespectrum
;;   (:use :cl)
;;   (:nicknames :wavespect)
;;   (:import-from :lisp-binary
;;                 :defbinary
;;                 :counted-array)
;;   (:import-from :array-operations
;;                 :linspace
;;                 :generate)
;;   (:export :spectral-point
;;            :spectral-point-ts
;;            :spectral-point-c
;;            :spectral-point-a1
;;            :spectral-point-a2
;;            :spectral-point-r1
;;            :spectral-point-r2
;;            :spectral-point-fsep
;;            :make-spect-point
           
;;            :spreading-function
;;            :directional-spectrum
;;            :spectrum))

(in-package :wavetools)

;;Defines the spectral data for a single reading from a buoy
(lisp-binary:defbinary spectral-point ()
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

(defun spectrum (spoint freqs dir-res)
  (let* (
         ;; Linear radial angle space
         (theta
          (aops:linspace
           0
           (* (/ (- dir-res 1) dir-res) 2 pi)
           dir-res))

         ;; Calculates a 2-D mesh of S(theta, freq)
         (spect
          (aops:generate (lambda (subscr)
                           (let ((pref (car subscr))
                                 (tref (cadr subscr)))
                             (directional-spectrum
                              (aref theta tref)                      ; theta
                              (aref (spectral-point-c spoint) pref)
                              (aref (spectral-point-a1 spoint) pref)
                              (aref (spectral-point-a2 spoint) pref)
                              (aref (spectral-point-r1 spoint) pref)
                              (aref (spectral-point-r2 spoint) pref))))
                         `(,(length freqs) ,(length theta))              ; size of array
                         :subscripts)))
    `(freqs ,freqs theta ,theta spect ,spect)))
