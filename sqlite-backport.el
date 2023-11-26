;;; sqlite-backport.el --- Backport SQLite database access from Emacs 29 -*- coding: utf-8; lexical-binding: t; -*-

;; Copyright (C) 2021-2022 Free Software Foundation, Inc.
;; Copyright (C) 2017 by Syohei YOSHIDA

;; Author: Syohei YOSHIDA <syohex@gmail.com>
;; Maintainer: 洪筱冰 <hxb@localhost.localdomain>
;; Homepage: https://github.com/hxb2012/sqlite-backport
;; Keywords: sqlite
;; Package-Requires: ((emacs "25.1"))
;; Version: 0.0.1

;; This file is NOT part of GNU Emacs.

;; This program is free software: you can redistribute it and/or
;; modify it under the terms of the GNU General Public License as
;; published by the Free Software Foundation, either version 3 of the
;; License, or (at your option) any later version.

;; This program is distributed in the hope that it will be useful, but
;; WITHOUT ANY WARRANTY; without even the implied warranty of
;; MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
;; General Public License for more details.

;; You should have received a copy of the GNU General Public License
;; along with GNU Emacs. If not, see <https://www.gnu.org/licenses/>.

;;; Commentary:

;; This file is based on the emacs-sqlite3 package written by Syohei
;; YOSHIDA <syohex@gmail.com>, which can be found at:

;;    https://github.com/syohex/emacs-sqlite3

;;; Code:

(defun sqlite-backport--include-dir ()
  ;; /usr/share/emacs/25.1/lisp/files.elc
  (let ((dirname (file-name-directory (locate-library "files"))))
    (dotimes (_ 4)
      (setq dirname (file-name-directory (directory-file-name dirname))))
    (file-name-as-directory (file-name-concat dirname "include"))))

(defun sqlite-backport--bootstrap ()
  (let* ((lispdir (file-name-directory (locate-library "sqlite-backport")))
         (libname (file-name-concat lispdir "sqlite-backport-module.so")))
    (if (file-exists-p libname)
        (load "sqlite-backport-module")
      (let ((default-directory lispdir))
        (if (eq
             0
             (shell-command
              (concat
               "LANG=C.utf8 cc -Wall -Wextra -Werror -shared -fPIC -o sqlite-backport-module.so sqlite-backport-module.c -I "
               (shell-quote-argument (sqlite-backport--include-dir))
               " `pkg-config --cflags --libs sqlite3`")
              "*compile-sqlite-backport-module*"))
            (load "sqlite-backport-module")
          (pop-to-buffer "*compile-sqlite-backport-module*"))))))

(with-eval-after-load 'sqlite-backport
  (when (not (locate-library "sqlite"))
    (sqlite-backport--bootstrap)))

;;;###autoload (autoload 'sqlite-open "sqlite-backport")
;;;###autoload (autoload 'sqlite-close "sqlite-backport")
;;;###autoload (autoload 'sqlite-execute "sqlite-backport")
;;;###autoload (autoload 'sqlite-select "sqlite-backport")
;;;###autoload (autoload 'sqlite-transaction "sqlite-backport")
;;;###autoload (autoload 'sqlite-commit "sqlite-backport")
;;;###autoload (autoload 'sqlite-rollback "sqlite-backport")
;;;###autoload (autoload 'sqlite-pragma "sqlite-backport")
;;;###autoload (autoload 'sqlite-next "sqlite-backport")
;;;###autoload (autoload 'sqlite-columns "sqlite-backport")
;;;###autoload (autoload 'sqlite-more-p "sqlite-backport")
;;;###autoload (autoload 'sqlite-finalize "sqlite-backport")
;;;###autoload (autoload 'sqlitep "sqlite-backport")
;;;###autoload (autoload 'sqlite-available-p "sqlite-backport")

;;;###autoload
(defmacro with-sqlite-transaction (db &rest body)
  "Execute BODY while holding a transaction for DB."
  (declare (indent 1) (debug (form body)))
  (let ((db-var (gensym))
        (func-var (gensym)))
    `(let ((,db-var ,db)
           (,func-var (lambda () ,@body)))
       (if (sqlite-available-p)
           (unwind-protect
               (progn
                 (sqlite-transaction ,db-var)
                 (funcall ,func-var))
             (sqlite-commit ,db-var))
         (funcall ,func-var)))))

(provide 'sqlite-backport)
;;; sqlite-backport.el ends here
