package lsm

import (
	"errors"
	"github.com/ryszard/goskiplist/skiplist"
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
	timestamp uint64 // 数据写入时的时间戳
}

// 索引信息
type Index struct {
	key    string
	offset uint32 // 记录下指定key在段文件中偏移
}

// LSM Tree
type Lsm struct {
	path     string
	memTable *skiplist.SkipList

	transLogFile       *os.File
	transLogStrictSync bool // transLog是否需要严格同步
	closed             bool
}

// 保存一组key,value
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
	l.closed = true

	l.SyncMemTable() // 关闭前同步数据

	var err error
	// 关闭日志文件
	err = l.transLogFile.Close()
	if err != nil {
		log.Fatal(err)
	}
	// 删除日志文件
	err = os.Remove(l.transLogFile.Name())
	if err != nil {
		log.Fatal(err)
	}

	// 删除锁文件
	err = os.Remove(path.Join(l.path, writeLockFile))
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

	buf := make([]byte, 0)      // 段文件内容
	indexBuf := make([]byte, 0) // 索引文件内容
	i := uint64(0)              // 记录当前已保存的数据条数

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

// 通过key获取值
func (l *Lsm) Get(key string) (string, bool) {
	memValue, ok := l.memTable.Get(key)
	if ok {
		return memValue.(Data).value, true
	}

	// 如果在memTable中没取到数据则需要去seg文件中进行查询
	valueTimestamp := uint64(0) // 当前值的时间
	value := ""                 // 最大时间对于的值

	// 根据得到的data来决定是否更新最终value的值
	var setValue = func(data Data) {
		if data.timestamp > valueTimestamp {
			value = data.value              // 更新值的内容
			ok = true                       // 已经找到了对应的值
			valueTimestamp = data.timestamp // 更新该值对应的时间戳
		}
	}

	indexFilesPath := getIndexFilesPath(l.path)
	// 根据所有的索引文件，去对应的段文件中检索数据
	for _, indexFilePath := range indexFilesPath {
		segFilePath := strings.Replace(indexFilePath, indexFileSuffix, segmentFileSuffix, -1)
		if _, err := os.Stat(strings.Replace(indexFilePath, indexFileSuffix, unavailableFileSuffix, -1)); !os.IsNotExist(err) {
			// 如果当前段文件存在对应的ua文件，则跳过此文件
			continue
		}

		var err error
		indexData, err := ioutil.ReadFile(indexFilePath)
		if err != nil {
			log.Fatal(err)
		}

		indices := getIndexList(indexData)
		length := len(indices)
		offsetLeft := uint32(0)  // 可检索范围内的最小索引下标
		offsetRight := uint32(0) // 可检索范围内的最大索引下标
		// 0或1个索引是没有意义的
		if length > 1 {
			// 超过范围，无法被找到，跳过该索引文件
			if key < indices[0].key || key > indices[length-1].key {
				continue
			}
			for i := 0; i < length; i++ {
				// 直接命中索引
				if indices[i].key == key {
					offsetLeft = indices[i].offset
					offsetRight = offsetLeft
					break
				}
				// 最后一个元素都没有命中，索引命中失败，需要全文检索
				if i == length-1 {
					break
				}
				// 索引范围命中，确定值应在指定的范围之中
				if indices[i].key < key && key < indices[i+1].key {
					offsetLeft = indices[i].offset
					offsetRight = indices[i+1].offset
					break
				}
			}
		}

		segFile, err := os.Open(segFilePath)
		if err != nil {
			log.Fatal(err)
		}
		size := getFileSize(segFile)

		if offsetLeft == 0 && offsetRight == 0 {
			// 1. 索引失效
			for {
				thisKey, data := readKeyAndData(segFile)
				if thisKey == key { // 取到对应的值
					setValue(data)
					break
				}

				position := getCurrentPosition(segFile)
				if position == size {
					// 最终也没能找到对应的值
					break
				}
			}
		} else if offsetLeft == offsetRight {
			// 2. 索引精确命中
			setCurrentPosition(segFile, offsetLeft)
			_, data := readKeyAndData(segFile)
			setValue(data)
		} else if offsetLeft < offsetRight {
			// 3. 索引范围命中
			setCurrentPosition(segFile, offsetLeft) // 从左开始查找

			for {
				thisKey, data := readKeyAndData(segFile)
				if thisKey == key {
					setValue(data)
					break
				}

				position := getCurrentPosition(segFile)
				if uint32(position) >= offsetRight {
					break
				}
			}
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
func (l *Lsm) backgroundMerge() {
	ticker := time.NewTicker(time.Second * mergeCheckInterval)
	for range ticker.C {
		if l.closed {
			return
		}
		// 存在不可用文件就跳过合并操作
		if isFileSuffixExist(l.path, unavailableFileSuffix) {
			continue
		}
		indexFilesPath := getIndexFilesPath(l.path)
		if len(indexFilesPath) > maxSegmentFileSize {
			file1, file2 := getTwoSmallFiles(indexFilesPath)

			segFile1, err := os.Open(file1)
			if err != nil {
				log.Fatal(err)
			}
			segFile2, err := os.Open(file2)
			if err != nil {
				log.Fatal(err)
			}

			segFile := createNewSegFile(l.path)
			merge(segFile1, segFile2, segFile)

			closeFile(segFile1)
			closeFile(segFile2)
			closeFile(segFile)

			// 移除新创建文件的不可用标志，表示新创建的文件已经可以被读取
			removeFile(strings.Replace(segFile.Name(), segmentFileSuffix, unavailableFileSuffix, -1))
			// 给旧的文件创建不可读标志
			uaFile1Path := strings.Replace(segFile1.Name(), segmentFileSuffix, unavailableFileSuffix, -1)
			uaFile1, err := os.Create(uaFile1Path)
			if err != nil {
				log.Fatal(err)
			}
			closeFile(uaFile1)
			uaFile2Path := strings.Replace(segFile2.Name(), segmentFileSuffix, unavailableFileSuffix, -1)
			uaFile2, err := os.Create(uaFile2Path)
			if err != nil {
				log.Fatal(err)
			}
			closeFile(uaFile2)
			// 在旧的段文件被打上废弃标签后，为了防止当前还有进程在读取此段文件，需要等待一段时间后再删除该文件
			time.Sleep(time.Second * waitOldSegFileDelTime)
			// 删除段文件，索引文件，不可用文件
			removeFile(segFile1.Name())
			removeFile(strings.Replace(segFile1.Name(), segmentFileSuffix, indexFileSuffix, -1))
			removeFile(uaFile1Path)
			removeFile(segFile2.Name())
			removeFile(strings.Replace(segFile2.Name(), segmentFileSuffix, indexFileSuffix, -1))
			removeFile(uaFile2Path)
		}
	}
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

	lockFilePath := path.Join(director, writeLockFile)
	if _, err := os.Stat(lockFilePath); !os.IsNotExist(err) {
		return nil, errors.New("Director " + director + " has been used for another LSM Tree")
	}
	lockFile, err := os.Create(lockFilePath)
	if err != nil {
		log.Fatal(err)
	}
	err = lockFile.Close()
	if err != nil {
		log.Fatal(err)
	}

	lsm := &Lsm{
		path:               director,
		memTable:           skiplist.NewStringMap(),
		transLogStrictSync: transLogStrictSync,
		closed:             false,
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
				if lsm.closed {
					return
				}
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

	go lsm.backgroundMerge()
	return lsm, nil
}
