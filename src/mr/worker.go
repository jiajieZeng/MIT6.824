package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// map
func StartMap(mapf func(string, string) []KeyValue, order *Order) {
	intermediate := []KeyValue{}
	filename := order.Filenames[0]
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("worker cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("worker cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)
	sort.Sort(ByKey(intermediate))

	nReduce := order.NReduce
	Bukets := make([][]KeyValue, nReduce)
	for _, kv := range intermediate {
		Bukets[ihash(kv.Key)%nReduce] = append(Bukets[ihash(kv.Key)%nReduce], kv)
	}
	for i := 0; i < nReduce; i++ {
		oname := "mr-tmp-" + strconv.Itoa(order.OrderID) + "-" + strconv.Itoa(i)
		ofile, _ := os.Create(oname)
		enc := json.NewEncoder(ofile)
		for _, kv := range Bukets[i] {
			enc.Encode(kv)
		}
		ofile.Close()
	}
}

// reduce
func StartReduce(reducef func(string, []string) string, order *Order) {
	intermediate := readLocal(order.Filenames)
	dir, _ := os.Getwd()
	ofile, err := ioutil.TempFile(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("startReduce: faile to create temp file", err)
	}
	i := 0
	sort.Sort(ByKey(intermediate))
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	ofile.Close()
	oname := fmt.Sprintf("mr-out-%d", order.OrderID)
	os.Rename(ofile.Name(), oname)

}

func readLocal(files []string) []KeyValue {
	var kva []KeyValue
	for _, filename := range files {
		file, _ := os.Open(filename)
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	return kva
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string, workerID string) {
	// Your worker implementation here.
	alive := true
	attemptCnt := 0
	for alive { // worker轮询
		attemptCnt++
		log.Println(workerID, ":  call for ", attemptCnt, " times")
		order := CallOrder()
		log.Println("worker ", workerID, "get order: ", order, " order Type", order.OrderType)
		switch order.OrderType {
		case MapOrder:
			StartMap(mapf, order)
			log.Printf("start %d map", order.OrderID)
			CallIsDone("Wow~~", order)
		case ReduceOrder:
			if order.OrderID <= 7 {
				continue
			}
			StartReduce(reducef, order)
			log.Printf("start %d map", order.OrderID)
			CallIsDone("Wowwww~~", order)
		case WaitingForMap:
			log.Println(workerID, " waiting for order")
			time.Sleep(time.Second)
		case WaitingForReduce:
			log.Println(workerID, " waiting for order")
			time.Sleep(time.Second)
		case KilledOrder:
			time.Sleep(time.Second)
			log.Println(workerID, " killed")
			alive = false
		}
		time.Sleep(time.Second)
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

// 请求任务
func CallOrder() *Order {
	args := ExampleArgs{}
	reply := Order{}
	call("Coordinator.DistributeOrder", &args, &reply)
	log.Println("CallOrder: worker get response", &reply)
	return &reply
}

// 完成任务
func CallIsDone(workerID string, order *Order) {
	args := order
	reply := &ExampleReply{}
	log.Printf("Worker finish order %d, order type %d", order.OrderID, order.OrderType)
	call("Coordinator.IsDone", &args, &reply)
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v+++++\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()
	// fmt.Println("call coordinator")
	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
