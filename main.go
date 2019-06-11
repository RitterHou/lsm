package main

import (
	"errors"
	"github.com/ryszard/goskiplist/skiplist"
	"io/ioutil"
	"log"
	"os"
	"path"
)

type Lsm struct {
	path     string
	memTable *skiplist.SkipList

	transLogFile *os.File
}

func (l *Lsm) Set(key string, value string) {
	l.appendTransLog(key, value) // 写transLog
	l.memTable.Set(key, value)
	if l.memTable.Len()%100 == 0 {
		memTableSize := l.getMemTableSize()
		if memTableSize > thresholdSize {
			l.Sync()
		}
	}
}

// 把当前memTable中的内容全部同步到SSTable中去
func (l *Lsm) Sync() {
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
	l.Sync() // 关闭前同步数据

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

	// 删除锁文件
	err = os.Remove(path.Join(l.path, lockFileName))
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
		value := iterator.Value().(string)
		memTableSize = memTableSize + uint64(len(key)) + uint64(len(value))
	}
	return memTableSize
}

// 创建SSTable
func (l *Lsm) createSortedStringTable() error {
	buf := make([]byte, 0)
	iter := l.memTable.Iterator()
	for iter.Next() {
		buf = append(buf, encodeKeyAndValue(iter.Key().(string), iter.Value().(string))...)
	}

	err := ioutil.WriteFile(path.Join(l.path, generateSegmentFileName(l.path)), buf, 0666)
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
	err = l.transLogFile.Sync()
	if err != nil {
		return err
	}
	return nil
}

func (l *Lsm) Get(key string) (string, bool) {
	value, ok := l.memTable.Get(key)
	if ok {
		return value.(string), true
	}
	return "", false
}

// 每一条记录都需要写到transLog保证数据不会因为内存断电而丢失
func (l *Lsm) appendTransLog(key string, value string) {
	var err error
	_, err = l.transLogFile.Write(encodeKeyAndValue(key, value))
	if err != nil {
		log.Fatal(err)
	}
	err = l.transLogFile.Sync()
	if err != nil {
		log.Fatal(err)
	}
}

func NewLsm(director string) (*Lsm, error) {
	if director == "" {
		dir, err := os.Getwd()
		if err != nil {
			log.Fatal(err)
		}
		director = dir
	}

	lockFilePath := path.Join(director, lockFileName)
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
		path:     director,
		memTable: skiplist.NewStringMap(),
	}

	transLogFilePath := path.Join(director, transLog)
	// 如果transLog文件存在则需要先从日志文件中恢复数据
	if _, err := os.Stat(transLogFilePath); !os.IsNotExist(err) {
		var err error
		logData, err := ioutil.ReadFile(transLogFilePath)
		if err != nil {
			log.Fatal(err)
		}
		if len(logData) > 0 {
			for len(logData) > 0 {
				key, value, length := decodeKeyAndValue(logData)
				lsm.memTable.Set(key, value)
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
		err = os.Remove(transLogFilePath)
		if err != nil {
			log.Fatal(err)
		}
	}
	transLogFile, err := os.Create(transLogFilePath)
	if err != nil {
		log.Fatal(err)
	}
	lsm.transLogFile = transLogFile
	return lsm, nil
}
