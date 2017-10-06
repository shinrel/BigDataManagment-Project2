#INPUT_PATH="/user/hadoop/tmp/testXML.txt"
KSEEDS_PATH="/user/hadoop/input/kseeds.txt"
INPUT_PATH="/user/hadoop/input/points.txt"
OUTPUT_PATH="/user/hadoop/output"
if [ $# -eq 0 ]
then
    echo "Usage: [Problem?] [max_iter] [distance-threshold] [5a/5b]"
    exit 1
fi
echo $1
hadoop fs -rmr -skipTrash $OUTPUT_PATH"*"
hadoop jar MapReduceRunner.jar $1 $KSEEDS_PATH $INPUT_PATH $OUTPUT_PATH $2 $3 $4
#echo "Result: \n"
#hadoop fs -cat $OUTPUT_PATH/part*
