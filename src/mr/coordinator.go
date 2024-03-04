package mr

import (
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var mutex sync.Mutex

type Coordinator struct {
	// Your definitions here.
	MapChannel    chan *Order // map任务
	ReduceChannel chan *Order // reduce任务
	NReduce       int         // reduce数量
	NMap          int         // map数量
	CoordStatus   CoordStatus // coord的状态
	UniqueOrderID int         // 分配给任务的唯一id
	MetaInfo      OrderMetaData
}

// 任务
type Order struct {
	OrderType OrderType // 任务类型
	Filenames []string  // 文件名
	OrderID   int       // 任务编号
	NReduce   int       // Reduce的数量
}

// 存储任务的元数据
type OrderInfo struct {
	Status    OrderStatus
	StartTime time.Time
	Order     *Order
}

type OrderMetaData struct {
	MetaData map[int]*OrderInfo
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) CheckProcess() {
	for {
		mutex.Lock()
		for _, value := range c.MetaInfo.MetaData {
			if value.Status != OrderWorking {
				continue
			}
			now := time.Now()
			dif := now.Sub(value.StartTime)
			if dif <= time.Second*10 {
				continue
			}
			value.Status = OrderWaiting
			if value.Order.OrderType == MapOrder {
				c.MapChannel <- value.Order
				log.Printf("Coordiantor restart Map order %d\n", value.Order.OrderID)
			} else {
				c.ReduceChannel <- value.Order
				log.Printf("Coordiantor restart Reduce order %d\n", value.Order.OrderID)
			}
		}
		mutex.Unlock()
		time.Sleep(time.Second)
	}
}

// 分配任务
func (c *Coordinator) DistributeOrder(args *ExampleArgs, reply *Order) error {
	log.Println("Coordinator work")
	mutex.Lock()
	defer mutex.Unlock()
	log.Printf("Coordinator status %d\n", c.CoordStatus)
	if c.CoordStatus == MapStat {
		if len(c.MapChannel) > 0 {
			*reply = *<-c.MapChannel
			if !c.MetaInfo.emitOrder(reply.OrderID) {
				log.Printf("duplicated order, order %d is running", reply.OrderID)
			}
		} else {
			reply.OrderType = WaitingForMap
			if c.MetaInfo.isAllDone(c.CoordStatus) {
				c.forward()
			}
		}
	} else if c.CoordStatus == ReduceStat {
		if len(c.ReduceChannel) > 0 {
			*reply = *<-c.ReduceChannel
			if !c.MetaInfo.emitOrder(reply.OrderID) {
				log.Printf("duplicated order, order %d is running", reply.OrderID)
			}
		} else {
			reply.OrderType = WaitingForReduce
			if c.MetaInfo.isAllDone(c.CoordStatus) {
				c.forward()
			}
		}
	} else {
		reply.OrderType = KilledOrder
	}
	return nil
}

// 生成ID
func (c *Coordinator) generateOrderID() int {
	x := c.UniqueOrderID
	c.UniqueOrderID++
	return x
}

// 制作map任务
func (c *Coordinator) makeMapOrder(filenames []string) {
	for _, v := range filenames {
		id := c.generateOrderID()
		order := Order{MapOrder, []string{v}, id, c.NReduce}
		orderInfo := OrderInfo{
			Status: OrderWaiting,
			Order:  &order,
		}
		c.MetaInfo.placeOrder(&orderInfo)
		log.Println("making map order", &order)
		c.MapChannel <- &order
	}
	log.Println("done with make map order")
	c.MetaInfo.isAllDone(c.CoordStatus)
}

// 制作reduce任务
func (c *Coordinator) makeReduceOrder() {
	for i := 0; i < c.NReduce; i++ {
		id := c.generateOrderID()
		log.Println("making reduce order :", id)
		order := Order{
			OrderType: ReduceOrder,
			OrderID:   id,
			Filenames: getIntermediateFile(i, "mr-tmp"),
		}
		orderInfo := OrderInfo{
			Status: OrderWaiting,
			Order:  &order,
		}
		c.MetaInfo.placeOrder(&orderInfo)
		c.ReduceChannel <- &order
	}
	log.Println("done with make reduce order")
	c.MetaInfo.isAllDone(c.CoordStatus)
}

// 临时文件
func getIntermediateFile(reduceNumber int, filename string) []string {
	var ans []string
	path, _ := os.Getwd()
	dir, _ := ioutil.ReadDir(path)
	for _, file := range dir {
		// fmt.Println(file.Name())
		if strings.HasPrefix(file.Name(), filename) && strings.HasSuffix(file.Name(), strconv.Itoa(reduceNumber)) {
			ans = append(ans, file.Name())
		}
	}
	return ans
}

// 放入任务管理桶
func (m *OrderMetaData) placeOrder(orderInfo *OrderInfo) bool {
	orderID := orderInfo.Order.OrderID
	metaData, _ := m.MetaData[orderID]
	if metaData != nil {
		log.Println("MetaData already has order id: ", orderID)
		return false
	} else {
		m.MetaData[orderID] = orderInfo
		return true
	}
}

// 发送任务
func (m *OrderMetaData) emitOrder(orderID int) bool {
	orderInfo, flag := m.MetaData[orderID]
	if !flag || orderInfo.Status != OrderWaiting {
		return false
	}
	orderInfo.Status = OrderWorking
	orderInfo.StartTime = time.Now()
	return true
}

// 检查是否全都完成
func (m *OrderMetaData) isAllDone(coordStatus CoordStatus) bool {
	dMap, udMap, dReduce, udReduce := 0, 0, 0, 0
	for _, v := range m.MetaData {
		if v.Order.OrderType == MapOrder {
			if v.Status == OrderDone {
				dMap++
			} else {
				udMap++
			}
		} else {
			if v.Status == OrderDone {
				dReduce++
			} else {
				udReduce++
			}
		}
	}
	log.Printf("Map: %d done vs. %d working, Reduce %d done vs. %d working", dMap, udMap, dReduce, udReduce)
	if coordStatus == MapStat {
		return (dMap > 0 && udMap == 0)
	} else if coordStatus == ReduceStat {
		return (dReduce > 0 && udReduce == 0)
	}
	return false
}

// 检查单个任务是否完成
func (c *Coordinator) IsDone(args *Order, reply *ExampleReply) error {
	mutex.Lock()
	defer mutex.Unlock()
	// log.Println("IsDone: ", args.OrderType)
	switch args.OrderType {
	case MapOrder:
		metaInfo, flag := c.MetaInfo.MetaData[args.OrderID]
		log.Println("IsDone", metaInfo, flag)
		if flag && metaInfo.Status == OrderWorking {
			metaInfo.Status = OrderDone
			log.Printf("Order %d finish Map\n", args.OrderID)
		} else {
			log.Printf("duplicated order %d one\n", args.OrderID)
		}
	case ReduceOrder:
		log.Printf("Order %d finish Reduce\n", args.OrderID)
		metaInfo, flag := c.MetaInfo.MetaData[args.OrderID]
		if flag && metaInfo.Status == OrderWorking {
			metaInfo.Status = OrderDone
		} else {
			log.Printf("duplicated order %d one\n", args.OrderID)
		}
	default:
		log.Panic("IsDone")
	}
	return nil
}

// 切换状态
func (c *Coordinator) forward() {
	if c.CoordStatus == MapStat {
		// reduce
		log.Println("forward: forward to Reuce Phase")
		c.makeReduceOrder()
		c.CoordStatus = ReduceStat
	} else if c.CoordStatus == ReduceStat {
		log.Println("forward: forward to done phase")
		c.CoordStatus = DoneStat
	}
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	// l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false
	// Your code here.
	mutex.Lock()
	defer mutex.Unlock()
	ret = (c.CoordStatus == DoneStat)
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		MapChannel:    make(chan *Order, len(files)),
		ReduceChannel: make(chan *Order, nReduce),
		CoordStatus:   MapStat,
		NMap:          len(files),
		NReduce:       nReduce,
		UniqueOrderID: 0,
		MetaInfo: OrderMetaData{
			MetaData: make(map[int]*OrderInfo, len(files)+nReduce),
		},
	}

	// Your code here.
	c.makeMapOrder(files)
	c.server()
	go c.CheckProcess()
	return &c
}
