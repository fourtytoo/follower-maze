(in-package :cl-user)

(require 'sb-bsd-sockets)

(defpackage :follower-maze-tests
  (:use :common-lisp
	:sb-unix
	:sb-bsd-sockets
	:rtest :follower-maze
	:net4cl)
  (:export))

(in-package :follower-maze-tests)

(defun send (socket fmt &rest args)
  (let ((stream (socket-stream socket)))
    (apply #'format stream fmt args)
    (terpri stream)
    (finish-output stream)))

(defun receive (socket)
  (follower-maze::line-remove-return (read-line (socket-stream socket))))

(defun receiven (socket n)
  (loop
     for i from 1 to n
     collect (receive socket)))

(defmacro with-source-socket ((socket &key (host "localhost")) &body forms)
  `(with-open-socket (,socket :host ,host :port follower-maze::*source-port*
			      :element-type 'character :buffering :none)
     ,@forms))

(defmacro with-user-socket ((socket id &key (host "localhost")) &body forms)
  `(with-open-socket (,socket :host ,host :port follower-maze::*users-port*
			      :element-type 'character :buffering :none)
     (send ,socket "~D" ,id)
     ,@forms))

(deftest connect.1
    (with-source-socket (s)
      t)
  t)

(deftest connect.2
    (with-user-socket (s 1)
      t)
  t)

(deftest broadcast.1
    (with-source-socket (s)
      (with-user-socket (u 2)
	(send s "1|B")
	(receive u)))
  "1|B")

(deftest broadcast.2
    (with-source-socket (s)
      (with-user-socket (u 2)
	(send s "1|B")
	(send s "2|B")
	(send s "3|B")
	(receiven u 3)))
  ("1|B" "2|B" "3|B"))

(deftest broadcast.3
    (with-source-socket (s)
      (with-user-socket (u 2)
	(send s "3|B")
	(send s "2|B")
	(send s "1|B")
	(receiven u 3)))
  ("1|B" "2|B" "3|B"))

(deftest broadcast.4
    (with-source-socket (s)
      (with-user-socket (u1 2)
	(with-user-socket (u2 5)
	  (send s "3|B")
	  (send s "2|B")
	  (send s "1|B")
	  (append (receiven u1 3) (receiven u2 3)))))
  ("1|B" "2|B" "3|B" "1|B" "2|B" "3|B"))

(deftest follow.1
    (with-source-socket (s)
      (with-user-socket (u 2)
	(send s "1|F|13|2")
	(receive u)))
  "1|F|13|2")

(deftest follow.2
    (with-source-socket (s)
      (with-user-socket (u 2)
	(send s "2|F|13|2")
	(send s "1|B")
	(receiven u 2)))
  ("1|B" "2|F|13|2"))

(deftest follow.3
    (with-source-socket (s)
      (with-user-socket (u 3)
	(send s "2|F|13|2")
	(send s "1|B")
	(receive u)))
  "1|B")

(deftest follow.4
    (with-source-socket (s)
      (with-user-socket (u1 3)
	(with-user-socket (u2 4)
	  (send s "3|F|13|4")
	  (send s "2|F|13|3")
	  (send s "1|B")
	  (list (list (receive u1) (receive u1))
		(list (receive u2) (receive u2))))))
  (("1|B" "2|F|13|3")
   ("1|B" "3|F|13|4")))

(deftest unfollow.1
    (with-source-socket (s)
      (with-user-socket (u 2)
	(send s "3|B")
	(send s "2|U|13|2")
	(send s "1|B")
	(receiven u 2)))
  ("1|B" "3|B"))

(deftest unfollow.2
    (with-source-socket (s)
      (with-user-socket (u 2)
	(send s "4|B")
	(send s "3|U|13|2")
	(send s "2|F|13|2")
	(send s "1|B")
	(receiven u 3)))
  ("1|B" "2|F|13|2" "4|B"))

(deftest message.1
    (with-source-socket (s)
      (with-user-socket (u 3)
	(send s "1|P|31|3")
	(receive u)))
  "1|P|31|3")

(deftest message.2
    (with-source-socket (s)
      (with-user-socket (u 3)
	(send s "1|P|13|4")
	(send s "2|B")
	(receive u)))
  "2|B")

(deftest message.3
    (with-source-socket (s)
      (with-user-socket (u1 3)
	(with-user-socket (u2 4)
	  (send s "3|B")
	  (send s "2|P|13|4")
	  (send s "1|P|13|3")
	  (list (receiven u1 2)
		(receiven u2 2)))))
  (("1|P|13|3" "3|B") ("2|P|13|4" "3|B")))

(deftest status.1
    (with-source-socket (s)
      (with-user-socket (u 3)
	(send s "1|S|17")
	(send s "2|B")
	(receive u)))
  "2|B")

(deftest status.2
    (with-source-socket (s)
      (with-user-socket (u 3)
	(send s "3|S|17")
	(send s "2|F|3|17")
	(send s "4|B")
	(send s "1|B")
	(receiven u 3)))
  ("1|B" "3|S|17" "4|B"))

(deftest status.3
    (with-source-socket (s)
      (with-user-socket (u 3)
	(send s "7|B")
	(send s "5|U|3|17")
	(send s "6|S|17")
	(send s "3|S|17")
	(send s "2|F|3|17")
	(send s "4|B")
	(send s "1|B")
	(receiven u 4)))
  ("1|B" "3|S|17" "4|B" "7|B"))

(deftest status.4
    (with-source-socket (s)
      (with-user-socket (u1 3)
	(with-user-socket (u2 5)
	  (send s "7|B")
	  (send s "6|S|5")
	  (send s "5|S|3")
	  (send s "2|F|3|5")
	  (send s "3|F|5|3")
	  (send s "4|B")
	  (send s "1|B")
	  (list (receiven u1 5)
		(receiven u2 5)))))
  (("1|B" "3|F|5|3" "4|B" "6|S|5" "7|B")
   ("1|B" "2|F|3|5" "4|B" "5|S|3" "7|B")))
