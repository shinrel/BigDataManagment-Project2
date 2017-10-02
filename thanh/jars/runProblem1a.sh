INPUT_PATH="/user/hadoop/input/project2/test/"
OUTPUT_PATH="/user/hadoop/output/project2/test"
#if [ $# -eq 0 ]
#then
#    echo "Usage: input Jar file path"
#    exit 1
#fi
hadoop fs -rmr -skipTrash $OUTPUT_PATH
hadoop jar MapReduceRunner.jar 1a $INPUT_PATH $OUTPUT_PATH
echo "Result: \n"
hadoop fs -cat $OUTPUT_PATH/part*
