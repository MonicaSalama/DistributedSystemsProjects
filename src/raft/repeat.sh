printf "run:"
date "+%Y-%m-%d %H:%M:%S"
if [ $# -eq 0 ]
then
	>&2 echo "error number of provided parameters are less than 1"
elif [ $# -eq 1 ]
then
#if user didn't specify what he wants to run we run all tests
	# echo "3A:"
	for ((n=0;n<$1;n++))
	do
		echo "test number $n"
		go test
	done
	# echo "3B:"
	# for ((n=0;n<$1;n++))
	# do
	# 	echo "test number $n"
	# 	go test -race -run 3B
	# done
	#to do: add 3C when completed
else
	echo $2
	for ((n=0;n<$1;n++))
	do
		echo "test number $n"
		go test -race -run $2
	done
fi