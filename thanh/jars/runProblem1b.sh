INPUT_PATH1="/user/hadoop/input/points.txt"
INPUT_PATH2="/user/hadoop/input/rectangles.txt"
OUTPUT_PATH="/user/hadoop/output"
hadoop fs -rmr -skipTrash $OUTPUT_PATH
hadoop jar MapReduceRunner.jar 1b $INPUT_PATH1 $INPUT_PATH2 $OUTPUT_PATH 0,0,2000,2000
#hadoop jar MapReduceRunner.jar 1b $INPUT_PATH1 $INPUT_PATH2 $OUTPUT_PATH
#echo "Result: \n"
#hadoop fs -cat $OUTPUT_PATH/part*
