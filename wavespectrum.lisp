(ql:quickload :array-operations :silent t)

(defstruct (spect-params (:constructor make-spect-params (f c a1 a2 r1 r2)))
  (f 0.0 :type float :read-only t)      ; frequency
  (c 0.0 :type float :read-only t)      ; non-directional spectral density
  (a1 0.0 :type float :read-only t)     ; mean wave direction
  (a2 0.0 :type float :read-only t)     ; principal wave direction
  (r1 0.0 :type float :read-only t)
  (r2 0.0 :type float :read-only t))

(defstruct (spectral-point (:constructor make-spectral-point (ts params &optional fsep)))
  "Defines the spectral data for a single reading from a buoy"
  (ts 0 :type integer :read-only t)
  (params #() :type vector)
  (fsep nil :read-only t))

(defun spoint-freqs (spoint)
  (map 'vector #'spect-params-f (spectral-point-params spoint)))

(defun spreading-function (theta a1 a2 r1 r2)
  (let ((alpha (- (* 1.5 pi) theta)))
    (/
     (+ 0.5
        (* r1 (cos (- alpha a1)))
        (* r2 (cos (* 2 (- alpha a2)))))
     pi)))

(defun directional-spectrum (theta sparams)
  (let ((c (spect-params-c sparams))
        (a1 (spect-params-a1 sparams))
        (a2 (spect-params-a2 sparams))
        (r1 (spect-params-r1 sparams))
        (r2 (spect-params-r2 sparams)))
    (* c (spreading-function theta a1 a2 r1 r2))))

(defun get-spectrum (spoint dir-res)
  (let* (
         ;; Linear radial angle space
         (theta
          (aops:linspace
           0
           (* (/ (- dir-res 1) dir-res) 2 pi)
           dir-res))

         ;; The frequencies in the spectrum
         (freqs
          (spoint-freqs spoint))

         ;; Calculates a 2-D mesh of S(theta, freq)
         (spect
          (aops:generate (lambda (subscr)
                           (let ((pref (car subscr))
                                 (tref (cadr subscr)))
                             (directional-spectrum
                              (aref theta tref)                      ; theta
                              (aref (spectral-point-params spoint) pref)))) ; sparams
                         `(,(length freqs) ,(length theta))              ; size of array
                         :subscripts)))
    `(freqs ,freqs theta ,theta spect ,spect)))
