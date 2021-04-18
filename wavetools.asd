(defsystem #:wavetools
  :author "Ayal Lutwak"
  :version "0.0.1"
  :description "A colletion of tools for analyzing wave data for the purpose of surf prediction"
  :licence "Public Domain / 0-clause MIT"
  :long-description
  "Wavetools is a library for wave analysis.

Blah blah blah.
Blah blah blah...."
  :depends-on (#:str
               #:lisp-binary
               #:array-operations
               #:sqlite
               #:mito
               #:cl-ppcre
               #:dexador
               #:lquery)
  :serial t
  :components ((:file "package")
               (:file "wavespectrum")
               (:file "station")
               (:file "station-cache")
               (:file "station-download")
               (:file "station-getter")
               (:static-file "cdipbuoy.py")))
