package mapreduce
import (
	"os"
	"encoding/json"
	"sort"
	"log"
)
// doReduce manages one reduce task: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// You will need to write this function.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTaskNumber) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
  var key_value []KeyValue
	for i := 0 ; i <  nMap ; i++ {
		file , err := os.Open(reduceName(jobName, i, reduceTaskNumber))
		if err != nil {
			log.Fatal(err)
		}
		dec := json.NewDecoder(file)

		// while the array contains values
		for dec.More() {
			var m KeyValue
			err := dec.Decode(&m)
			key_value = append(key_value,m)
			if err != nil {
				log.Fatal(err)
			}
		}
		file.Close()
	}
	sort.Sort(ByKey(key_value))
  out_file, err := os.Create(outFile)
	if err != nil {
		os.Exit(1)
	}
	enc := json.NewEncoder(out_file)

	var values_list []string
	for i:= 0 ; i < len(key_value) ; i++ {
		if (i == len(key_value)-1 || key_value[i].Key != key_value[i+1].Key) {
			values_list = append(values_list, key_value[i].Value)
			value := reduceF(key_value[i].Key, values_list)
			kv := KeyValue{Key: key_value[i].Key, Value: value}
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatal(err)
			}
			values_list = values_list[:0]
		} else {
			values_list = append(values_list, key_value[i].Value)
		}
	}
	out_file.Close()

}

type ByKey []KeyValue

func (s ByKey) Len() int {
    return len(s)
}
func (s ByKey) Swap(i, j int) {
    s[i], s[j] = s[j], s[i]
}
func (s ByKey) Less(i, j int) bool {
    return s[i].Key <  s[j].Key
}
