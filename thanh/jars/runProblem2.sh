#INPUT_PATH="/user/hadoop/tmp/testXML.txt"
INPUT_PATH="/user/hadoop/tmp/alljson.txt"
OUTPUT_PATH="/user/hadoop/tmp/out"
#if [ $# -eq 0 ]
#then
#    echo "Usage: input Jar file path"
#    exit 1
#fi
hadoop fs -rmr -skipTrash $OUTPUT_PATH
hadoop jar MapReduceRunner.jar 2 -Dmapred.map.tasks=20 $INPUT_PATH $OUTPUT_PATH
echo "Result: \n"
hadoop fs -cat $OUTPUT_PATH/part*
