(in-package :cl-user)

(require 'sb-bsd-sockets)

(defpackage :follower-maze
  (:use :common-lisp
	:sb-unix
	:sb-bsd-sockets)
  (:export #:serve-clients
	   #:close-all-sockets))

(in-package :follower-maze)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defvar *source-port* 9090)

(defvar *users-port* 9099)

(defvar *string->event-type-alist*
  '(("F" :follow) ("U" :unfollow) ("B" :broadcast) ("P" :message) ("S" :status)))
(defvar *event-type->string-alist*
  (mapcar #'(lambda (e)
	      (list (cadr e) (car e)))
	  *string->event-type-alist*))

(defvar *debug* nil)
(defun dtrace (fmt &rest args)
  (when *debug*
    (format t "[~D] " (get-universal-time))
    (apply #'format t fmt args)
    (terpri)
    (finish-output)))

(defvar *sockets* '()
  "List of currently open/used sockets.  For debugging purposes.")

(defun close-socket (socket)
  (socket-close socket)
  (setf *sockets* (delete socket *sockets*)))

(defun close-all-sockets ()
  (loop
     for s = (car *sockets*)
     while s
     do
       (format t "closing ~S~%" s)
       (close-socket s)))

(defun make-server-socket (port)
  (let ((socket (make-instance 'inet-socket :type :stream :protocol :tcp)))
    (push socket *sockets*)
    (socket-bind socket (vector 0 0 0 0) port)
    (socket-listen socket 5)
    socket))

(let ((buffer (make-array 1 :element-type 'character)))
  (defun socket-read-char (socket)
    (let ((n (unix-read (socket-file-descriptor socket) (sb-sys:vector-sap buffer) 1)))
      (when (or (not n) (< n 1))
	(error 'end-of-file))
      (aref buffer 0))))

(defun socket-read-line (socket)
  (with-output-to-string (out)
    (loop
       for c = (socket-read-char socket)
       until (char= c #\newline)
       do (write-char c out))))

(defun socket-write-line (socket line)
  (let ((fd (socket-file-descriptor socket))
	(buf (sb-ext:string-to-octets line :external-format :utf8))
	(crlf #.(sb-ext:string-to-octets (concatenate 'string '(#\return #\newline))
					 :external-format :utf8)))
    (unix-write fd (sb-sys:vector-sap buf) 0 (length line))
    (unix-write fd (sb-sys:vector-sap crlf) 0 2)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defstruct queue
  first
  last)

(defgeneric queue-append (queue objects))
(defgeneric queue-pop (queue))
(defgeneric queue-empty-p (queue))

(defmethod queue-append ((queue queue) (objects list))
  (cond ((null (queue-first queue))
	 (setf (queue-first queue) objects
	       (queue-last queue) (last objects)))
	(t
	 (setf (cdr (queue-last queue)) objects
	       (queue-last queue) (last objects))))
  queue)

(defmethod queue-append ((queue queue) object)
  (queue-append queue (list object)))

(defmethod queue-pop ((queue queue))
  (prog1 (car (queue-first queue))
    (setf (queue-first queue) (cdr (queue-first queue)))))

(defmethod queue-empty-p ((queue queue))
  (null (queue-first queue)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defclass socket-based-mixin ()
  ((socket :accessor real-socket
	   :initarg :socket)))

(defclass listener (socket-based-mixin)
  ())

(defclass client (socket-based-mixin)
  ())

(defmethod client-close ((client client))
  (close-socket (real-socket client)))

(defmethod client-read-char ((client client))
  (socket-read-char (real-socket client)))

(defmethod client-read-line ((client client))
  (socket-read-line (real-socket client)))

(defmethod client-write-line ((client client) line)
  (socket-write-line (real-socket client) line))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defclass source (client)
  ((events :initform (make-events-queue)
	   :reader source-events)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defclass user (client)
  ((id :reader user-id
       :initarg :id)
   (events :initform (make-queue))
   (followed :initform '()
	     :reader user-followed)))

(defmethod print-object ((object user) stream)
  (if *print-readably*
      (call-next-method)
      (print-unreadable-object (object stream :type t :identity t)
	(format stream "id=~D" (user-id object)))))

(defun read-user-id (user)
  (parse-integer (line-remove-return (client-read-line user))))

(defmethod initialize-instance ((obj user) &rest args)
  (declare (ignore args))
  (call-next-method)
  (unless (slot-boundp obj 'id)
    (setf (slot-value obj 'id) (read-user-id obj))))

(defun format-event (event)
  (format nil "~A|~A~@[|~A~]~@[|~A~]"
	  (event-serial event)
	  (cadr (assoc (event-type event) *event-type->string-alist*))
	  (event-from event)
	  (event-to event)))

(defmethod send-event ((user user) event)
  (dtrace "~S -> ~S" (format-event event) (user-id user))
  (client-write-line user (format-event event)))

(defmethod user-enqueue-event ((user user) event)
  (queue-append (slot-value user 'events) event))

(defmethod user-dequeue-event ((user user))
  (queue-pop (slot-value user 'events)))

(defmethod has-pending-events? ((user user))
  (not (queue-empty-p (slot-value user 'events))))

(defmethod user-add-followed ((user user) id)
  (with-slots (followed) user
    (pushnew id followed)))

(defmethod user-removed-followed ((user user) id)
  (with-slots (followed) user
    (setf followed (delete id followed))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defun split-string-at-char (string separator &key escape skip-empty)
  "Split STRING at SEPARATORs and return a list of the substrings.  If
SKIP-EMPTY is true then filter out the empty substrings.  If ESCAPE is
not nil then split at SEPARATOR only if it's not preceded by ESCAPE."
  (declare (type string string) (type character separator))
  (labels ((next-separator (beg)
             (let ((pos (position separator string :start beg)))
	       (if (and escape
			pos
			(plusp pos)
			(char= escape (char string (1- pos))))
		   (next-separator (1+ pos))
		   pos)))
           (parse (beg)
             (cond ((< beg (length string))
                    (let* ((end (next-separator beg))
                           (substring (subseq string beg end)))
                      (cond ((and skip-empty (string= "" substring))
                             (parse (1+ end)))
                            ((not end)
                             (list substring))
                            (t
			     (cons substring (parse (1+ end)))))))
                   (skip-empty
		    '())
                   (t
		    (list "")))))
    (parse 0)))

(defun line-remove-return (line)
  (let ((length (length line)))
    (if (and (plusp length)
	     (char= (char line (1- length)) #\return))
	(subseq line 0 (1- length))
	line)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defun parse-event (string)
  (split-string-at-char string #\|))

(defstruct event
  serial
  type
  from
  to)

(defun receive-event (source)
  (let ((line (client-read-line source)))
    (destructuring-bind (seq# type &optional from to)
	(parse-event (line-remove-return line))
      (setf type (cadr (assoc type *string->event-type-alist* :test #'string-equal)))
      (assert type)
      (make-event :serial (parse-integer seq#)
		  :type type
		  :from (when from (parse-integer from))
		  :to (when to (parse-integer to))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defstruct events-queue
  (expected 1)
  events)

(defun events-queue-enqueue (queue event)
  (labels ((enqueue (list)
	     (cond ((endp list)
		    (list event))
		   ((< (event-serial event)
		    (event-serial (car list)))
		    (cons event list))
		   (t (cons (car list) (enqueue (cdr list)))))))
    (setf (events-queue-events queue)
	  (enqueue (events-queue-events queue))))
  event)

(defun events-queue-dequeue (queue)
  (when (and (events-queue-events queue)
	     (= (events-queue-expected queue)
		(event-serial (car (events-queue-events queue)))))
    (incf (events-queue-expected queue))
    (pop (events-queue-events queue))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defclass users-pool ()
  ((table :initform (make-hash-table))))

(defmethod pool-map (function (pool users-pool))
  (with-slots (table) pool
    (loop
       for key being the hash-key of table using (hash-value value)
       collect (funcall function value))))

(defmethod pool-list-users ((pool users-pool))
  (pool-map #'identity pool))

(defmethod pool-add-user ((pool users-pool) user)
  (with-slots (table) pool
    (assert (not (gethash (user-id user) table)))
    (setf (gethash (user-id user) table) user))
  (values))

(defmethod pool-remove-user ((pool users-pool) user)
  (with-slots (table) pool
    (remhash (user-id user) table)))

(defmethod pool-find-user ((pool users-pool) id)
  (with-slots (table) pool
    (gethash id table)))

(defmethod pool-list-users-with-pending-events ((pool users-pool))
  (remove-if #'(lambda (u)
		 (not (has-pending-events? u)))
	     (pool-list-users pool)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defun select-on-sockets (readers writers)
  "Perform a select(2) on READERS and WRITERS.  Return two lists: the
list of sockets ready to be read and a list of sockets ready to be
written."
  (flet ((fill-fd-set (set sockets)
	   (fd-zero set)
	   (dolist (socket sockets)
	     (fd-set (socket-file-descriptor socket) set))
	   set)
	 (fds-set? (set sockets)
	   (loop
	      for socket in sockets
	      when (sb-unix:fd-isset (socket-file-descriptor socket) set)
	      collect socket)))
  (sb-alien:with-alien ((rfds (sb-alien:struct sb-unix:fd-set))
			(wfds (sb-alien:struct sb-unix:fd-set)))
    (fill-fd-set rfds readers)
    (fill-fd-set wfds writers)
    (multiple-value-bind (count err)
        (unix-fast-select
	 (1+ (apply #'max (mapcar #'socket-file-descriptor (append readers writers))))
         (sb-alien:addr rfds)
	 (sb-alien:addr wfds)
	 nil nil nil)
      (declare (ignore count))
      (unless (zerop err)
	(error "select error ~D" err))
      (values (fds-set? rfds readers)
	      (fds-set? wfds writers))))))

(defun select-on-clients (input-clients output-clients)
  "Accept two lists of CLIENT objects; one for input and one for
output.  Perform a select(2) on their socket and return those that are
ready to be read or written in two separate lists."
  (multiple-value-bind (readable writable)
      (select-on-sockets (mapcar #'real-socket input-clients)
			 (mapcar #'real-socket output-clients))
    (values (mapcar #'(lambda (socket)
			(find socket input-clients :key #'real-socket))
		    readable)
	    (mapcar #'(lambda (socket)
			(find socket output-clients :key #'real-socket))
		    writable))))

(defun accept-user-connection (listener)
  (let ((socket (socket-accept (real-socket listener))))
    (push socket *sockets*)
    (make-instance 'user :socket socket)))

(defun accept-source-connection (listener)
  (let ((socket (socket-accept (real-socket listener))))
    (push socket *sockets*)
    (make-instance 'source :socket socket)))

(defun serve-clients ()
  (close-all-sockets)			; from previous run
  (let ((source-listener (make-instance 'listener :socket (make-server-socket *source-port*)))
	(users-listener (make-instance 'listener :socket (make-server-socket *users-port*)))
	(source nil)
	(users (make-instance 'users-pool)))
    (flet ((process-event (event)
	     (case (event-type event)
	       (:follow
	       	(let ((follower (pool-find-user users (event-from event)))
		      (followed (pool-find-user users (event-to event))))
		  (dtrace "~S(~D) will follow ~S(~D)" follower (event-from event)
			  followed (event-to event)) ; -wcp14/6/13.
	       	  (when follower
	       	    (user-add-followed follower (event-to event)))
		  (when followed
	       	    (user-enqueue-event followed event))))
	       (:unfollow
	       	(let ((user (pool-find-user users (event-from event))))
		  (dtrace "~S(~D) stops following ~S" user
			  (event-from event) (event-to event))
	       	  (when user
	       	    (user-removed-followed user (event-to event)))))
	       (:broadcast
		(pool-map #'(lambda (user)
			      (user-enqueue-event user event))
			  users))
	       (:message
	       	(let ((user (pool-find-user users (event-to event))))
	       	  (when user
	       	    (user-enqueue-event user event))))
	       (:status
	       	(let ((from (event-from event)))
		  (dtrace "~S(~D) status change" from (event-from event)) ; -wcp14/6/13.
	       	  (pool-map #'(lambda (user)
				(when (some #'(lambda (u) (= u from))
					    (user-followed user))
				  (dtrace "notified ~S of ~S state change" user from) ; -wcp14/6/13.
				  (user-enqueue-event user event)))
	       		    users))))))
      (loop
	 (multiple-value-bind (readable writable)
	     (select-on-clients (list* (or source source-listener) users-listener
				       (pool-list-users users))
				(pool-list-users-with-pending-events users))
	   (dolist (user writable)
	     (handler-case (send-event user (user-dequeue-event user))
	       (sb-int:simple-stream-error (c)
		 (dtrace "error=~A" c)
		 (dtrace "LOST user ~D" (user-id user))
		 (client-close user)
		 (pool-remove-user users user))))
	   (dolist (listener readable)
	     (cond ((and (not source)	; don't accept more than one source
			 (eq listener source-listener))
		    (dtrace "CONNECTED source")
		    (setf source (accept-source-connection source-listener)))
		   ((eq listener source)
		    (handler-case
			(let ((event (receive-event source)))
			  (events-queue-enqueue (source-events source) event))
		      (end-of-file ()
			(dtrace "LOST source")
			(setf source nil)))
		    (when source
		      (loop
			 for e = (events-queue-dequeue (source-events source))
			 while e
			 do (process-event e))))
		   ((eq listener users-listener)
		    (let ((user (accept-user-connection users-listener)))
		      (dtrace "CONNECTED user ~D" (user-id user))
		      (pool-add-user users user)))
		   (t			; we have lost a client
		    (handler-case (client-read-char listener)
		      (end-of-file ()
			(dtrace "LOST user ~D" (user-id listener))
			(client-close listener)
			(pool-remove-user users listener)))))))))))

