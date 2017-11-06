A Synapse Write Performance Study
=====

A simple set of tests used to understand how write performance varies with the
size of the database in a Synapse (https://github.com/vertexproject/synapse) hypergraph.

In order to understand any overhead due to Synapse, plain Python and C programs are 
measured as well.

Prerequisites
-----
* Ubuntu running systemd. (It shouldn't be too hard to port to Mac or another Linux).
* Postgresql 9.6 installed.  It should have a user called "synapse" with
  password "synapse" with access to a database called "db".
* Python 3.6 with `synapse` and `plotly` packages installed.  An activated
  virtualenv will do fine.  The C version of messagepack must be installed.
  (Download it and run `python setup.py install`)
* The user either needs passwordless sudo for the systemctl command (see
  https://serverfault.com/questions/772778/allowing-a-non-root-user-to-restart-a-service) or
  needs to run the `full_run.sh` script with sudo.

How to Run
-----
* cd to `write_test`
* Build the C lmdb program: `make`
* `./full_run.sh 1024 run1.txt` will have all the tests write 1 GiB.  It will run for several hours.
* `python ./write_test.py run1.txt run1.txt` will generate the pretty chart from the graph.


