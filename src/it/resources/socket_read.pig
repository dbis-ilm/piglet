A = SOCKET_READ 'tcp://localhost:9999' MODE ZMQ USING PigStream(',') AS (f1:double, f2:double, f3:double);
B = FILTER A BY f2>0;
DUMP B;
