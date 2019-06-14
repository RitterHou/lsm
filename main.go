package main

import (
	"github.com/ryszard/goskiplist/skiplist"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
	"strings"
	"time"
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

type Data struct {
	value     string
	timestamp uint64
}

type Index struct {
	key    string
	offset uint32
}

type Lsm struct {
	path     string
	memTable *skiplist.SkipList

	transLogFile       *os.File
	transLogStrictSync bool // transLog是否需要严格同步
}

func (l *Lsm) Set(key string, value string) {
	data := Data{value: value, timestamp: uint64(time.Now().UnixNano())}
	l.appendTransLog(key, data) // 写transLog
	l.memTable.Set(key, data)
	if l.memTable.Len()%memTableCheckInterval == 0 {
		memTableSize := l.getMemTableSize()
		if memTableSize > thresholdSize {
			l.SyncMemTable()
		}
	}
}

// 把当前memTable中的内容全部同步到SSTable中去
func (l *Lsm) SyncMemTable() {
	var err error
	err = l.createSortedStringTable()
	if err != nil {
		log.Fatal(err)
	}
	// 重置memTable
	l.memTable = skiplist.NewStringMap()
	err = l.resetTransLogFile()
	if err != nil {
		log.Fatal(err)
	}
}

// 关闭LSM，释放占用的资源
func (l *Lsm) Close() {
	l.SyncMemTable() // 关闭前同步数据

	// 获取日志文件的绝对路径
	transLogFilePath := GetFilePath(l.transLogFile)
	var err error
	// 关闭日志文件
	err = l.transLogFile.Close()
	if err != nil {
		log.Fatal(err)
	}
	// 删除日志文件
	err = os.Remove(transLogFilePath)
	if err != nil {
		log.Fatal(err)
	}
}

// 获取memTable所占用的空间大小
func (l *Lsm) getMemTableSize() uint64 {
	var memTableSize uint64 // 内存中占用的空间
	iterator := l.memTable.Iterator()
	for iterator.Next() {
		key := iterator.Key().(string)
		data := iterator.Value().(Data)
		memTableSize = memTableSize + uint64(len(key)) + uint64(len(data.value)) + 8
	}
	return memTableSize
}

// 创建SSTable
func (l *Lsm) createSortedStringTable() error {
	// 没有数据则无需保存
	if l.memTable.Len() == 0 {
		return nil
	}

	buf := make([]byte, 0)
	indexBuf := make([]byte, 0)
	i := uint64(0)

	iter := l.memTable.Iterator()
	for iter.Next() {
		key := iter.Key().(string)
		data := iter.Value().(Data)

		if i%indexOffset == 0 || i+1 == uint64(l.memTable.Len()) {
			// 把段文件中的稀疏的key的offset信息写到索引中
			indexOffset := append(addBufHead([]byte(key)), uint32ToBytes(uint32(len(buf)))...)
			indexBuf = append(indexBuf, indexOffset...)
		}
		i += 1
		buf = append(buf, encodeKeyAndData(key, data)...)
	}

	// 段文件
	segmentFileName := generateSegmentFileName(l.path)
	err := ioutil.WriteFile(path.Join(l.path, segmentFileName), buf, 0666)
	if err != nil {
		return err
	}

	// 索引文件
	indexFileName := strings.Replace(segmentFileName, segmentFileSuffix, indexFileSuffix, -1)
	err = ioutil.WriteFile(path.Join(l.path, indexFileName), indexBuf, 0666)
	if err != nil {
		return err
	}
	return nil
}

// 重置日志文件
func (l *Lsm) resetTransLogFile() error {
	var err error
	err = l.transLogFile.Truncate(0)
	if err != nil {
		return err
	}
	_, err = l.transLogFile.Seek(0, 0)
	if err != nil {
		return err
	}
	if l.transLogStrictSync {
		err = l.transLogFile.Sync()
		if err != nil {
			return err
		}
	}
	return nil
}

func (l *Lsm) Get(key string) (string, bool) {
	memValue, ok := l.memTable.Get(key)
	if ok {
		return memValue.(Data).value, true
	}

	// 如果在memTable中没取到数据则需要去seg文件中进行查询
	valueTimestamp := uint64(0) // 当前值的时间
	value := ""                 // 最大时间对于的值

	// 根据得到的data来决定是否更新value的值
	var setValue = func(data Data) {
		if data.timestamp > valueTimestamp {
			value = data.value
			ok = true
			valueTimestamp = data.timestamp
		}
	}

	indexFilesPath := getIndexFilesPath(l.path)
	for _, indexFilePath := range indexFilesPath {
		segFilePath := strings.Replace(indexFilePath, indexFileSuffix, segmentFileSuffix, -1)

		var err error
		indexData, err := ioutil.ReadFile(indexFilePath)
		if err != nil {
			log.Fatal(err)
		}

		indices := getIndexList(indexData)
		length := len(indices)
		low := uint32(0)
		high := uint32(0)
		if length > 1 { // 0或1个索引是没有意义的
			// 超过范围，无法被找到，跳过该索引文件
			if key < indices[0].key || key > indices[length-1].key {
				continue
			}
			for i := 0; i < length; i++ {
				if indices[i].key == key { // 直接命中索引
					low = indices[i].offset
					high = low
					break
				}
				if i == length-1 {
					// 最后一个元素都没有命中，不再需要范围查询了
					break
				}
				if indices[i].key < key && key < indices[i+1].key {
					low = indices[i].offset
					high = indices[i+1].offset
					break
				}
			}
		}

		segFile, err := os.Open(segFilePath)
		if err != nil {
			log.Fatal(err)
		}
		fileInfo, err := segFile.Stat()
		if err != nil {
			log.Fatal(err)
		}
		size := fileInfo.Size()

		if low == 0 && high == 0 { // 1. 索引失效
			//start := time.Now().UnixNano()
			for {
				thisKey, data := readKeyAndData(segFile)
				if thisKey == key {
					setValue(data)
					break
				}

				// 从当前偏移改变0，还是当前偏移
				pos, _ := segFile.Seek(0, io.SeekCurrent)
				if pos >= size {
					break
				}
			}
			// 索引失败的查询时间差不多是其它的1000倍
			//fmt.Printf("索引失败：%d\n", time.Now().UnixNano()-start)
		} else if low == high { // 2. 索引命中
			//start := time.Now().UnixNano()
			_, err = segFile.Seek(int64(low), 0)
			if err != nil {
				log.Fatal(err)
			}
			_, data := readKeyAndData(segFile)
			setValue(data)
			//fmt.Printf("索引命中：%d\n", time.Now().UnixNano()-start)
		} else if low < high { // 3. 索引范围命中
			//start := time.Now().UnixNano()
			_, err = segFile.Seek(int64(low), 0)
			if err != nil {
				log.Fatal(err)
			}

			for {
				thisKey, data := readKeyAndData(segFile)
				if thisKey == key {
					setValue(data)
					break
				}

				// 从当前偏移改变0，还是当前偏移
				pos, _ := segFile.Seek(0, io.SeekCurrent)
				if uint32(pos) >= high {
					break
				}
			}
			//fmt.Printf("范围索引命中：%d\n", time.Now().UnixNano()-start)
		}

		err = segFile.Close()
		if err != nil {
			log.Fatal(err)
		}
	}
	return value, ok
}

// 每一条记录都需要写到transLog保证数据不会因为内存断电而丢失
func (l *Lsm) appendTransLog(key string, data Data) {
	var err error
	_, err = l.transLogFile.Write(encodeKeyAndData(key, data))
	if err != nil {
		log.Fatal(err)
	}
	// 如果开启了严格同步，则每一条日志都需要同步到磁盘
	if l.transLogStrictSync {
		err = l.transLogFile.Sync()
		if err != nil {
			log.Fatal(err)
		}
	}
}

// 恢复transLog中的数据，并把其数据写到SSTable中
func restoreTransLogData(lsm *Lsm, transLogFilePath string) {
	logData, err := ioutil.ReadFile(transLogFilePath)
	if err != nil {
		log.Fatal(err)
	}
	if len(logData) > 0 {
		for len(logData) > 0 {
			key, data, length := decodeKeyAndData(logData)
			lsm.memTable.Set(key, data)
			logData = logData[length:]
		}
		// 把恢复的数据写到SSTable中
		err = lsm.createSortedStringTable()
		if err != nil {
			log.Fatal(err)
		}
		// 日志数据恢复完毕重置memTable
		lsm.memTable = skiplist.NewStringMap()
	}
}

// 后台对数据文件进行合并
func backgroundMerge(director string) {

}

// 新建一个LSM，数据文件的目录地址，是否开启严格的事务日志同步模式
func NewLsm(director string, transLogStrictSync bool) (*Lsm, error) {
	if director == "" {
		dir, err := os.Getwd()
		if err != nil {
			log.Fatal(err)
		}
		director = dir
	}

	lsm := &Lsm{
		path:               director,
		memTable:           skiplist.NewStringMap(),
		transLogStrictSync: transLogStrictSync,
	}
	transLogFilePath := path.Join(director, transLog)
	// 如果transLog文件存在则需要先从日志文件中恢复数据
	if _, err := os.Stat(transLogFilePath); !os.IsNotExist(err) {
		restoreTransLogData(lsm, transLogFilePath)
		err := os.Remove(transLogFilePath)
		if err != nil {
			log.Fatal(err)
		}
	}
	transLogFile, err := os.Create(transLogFilePath)
	if err != nil {
		log.Fatal(err)
	}
	lsm.transLogFile = transLogFile

	// 如果没有开启严格的同步模式，则需要异步的transLog数据同步
	if !lsm.transLogStrictSync {
		go func() {
			ticker := time.NewTicker(time.Second * transLogAsyncInterval)
			for range ticker.C {
				// 每隔指定时间把日志数据落盘
				err = lsm.transLogFile.Sync()
				if err != nil {
					// 在LSM被关闭时，日志文件会被关闭，此时退出异步数据落盘协程
					if err.Error() == "sync "+lsm.transLogFile.Name()+": file already closed" {
						log.Println("Async transLog synchronize goroutine closed.")
						return
					}
					log.Fatal(err)
				}
			}
		}()
	}

	go backgroundMerge(lsm.path)
	return lsm, nil
}
