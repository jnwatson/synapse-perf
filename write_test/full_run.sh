# First parameter is # of mebibytes to write.  Second parameter is where the output goes.
if (( $# != 2 )); then
    echo "Usage: full_run.sh <SIZE in MiB> <outfile>"
    exit
fi
SIZE=$1
OUTPUT=$2
rm $OUTPUT

# Run the postgres tests first so we don't have postgres running while the other tests are going.
systemctl start postgresql@9.6-main
python write_test.py outfile $SIZE --delete-first --use-postgresql | tee -a $OUTPUT
systemctl restart postgresql@9.6-main
python write_test.py outfile $SIZE --delete-first --use-synapse --use-postgresql | tee -a $OUTPUT
systemctl stop postgresql@9.6-main

rm -f c.lmdb
./c_lmdb_write_test_exe $SIZE outfile | tee -a $OUTPUT
rm -f c.lmdb

rm outfile
python write_test.py outfile $SIZE --delete-first | tee -a $OUTPUT
rm outfile
python write_test.py outfile $SIZE --delete-first --use-sqlite | tee -a $OUTPUT
rm outfile

python write_test.py outfile $SIZE --delete-first --use-synapse | tee -a $OUTPUT
rm outfile
python write_test.py outfile $SIZE --delete-first --use-synapse --use-sqlite | tee -a $OUTPUT
rm outfile

