package main

import (
	"fmt"
	"os"
)

// 如果文件不存在，会创建文件，或者在写入内容之前截断现有文件。最重要的是，除非调用 fsync（Go 中的 fp.Sync()），否则数据不会持久存在。
// 它更新的是整体内容；只能用于微小数据
// 如果需要更新旧文件，必须在内存中读取和修改，然后覆盖旧文件。如果应用程序在覆盖旧文件时崩溃了怎么办？
// 如果应用程序需要并发访问数据，如何防止读取器获取混合数据和写入器进行冲突操作?这就是为什么大多数数据库都是客户端-服务器的原因，你需要一个服务器来协调并发客户端。
func SaveData1(path string, data []byte) error {
	fp, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0664)
	if err != nil {
		return err
	}
	defer fp.Close()

	_, err = fp.Write(data)
	if err != nil {
		return err
	}
	return fp.Sync() // fsync
}

// 不就地更新数据可以解决很多问题。你可以写入一个新文件，然后删除旧文件。
// 不动旧文件数据意味着
// 1. 如果更新被中断，你可以从旧文件中恢复，因为它仍然完好无损。
// 2. 并发阅读器不会获取写入一半的数据。
// 问题在于reader如何找到新文件。常见的模式是将新文件重命名为旧文件路径
func SaveData2(path string, data []byte) error {
	tmp := fmt.Sprintf("%s.tmp.%d", path, randomInt())
	fp, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0664)
	if err != nil {
		return err
	}
	defer func() {
		fp.Close()
		if err != nil {
			os.Remove(tmp)
		}
	}()

	_, err = fp.Write(data)
	if err != nil {
		return err
	}
	err = fp.Sync() // fsync
	if err != nil {
		return err
	}

	return os.Rename(tmp, path)
}

func randomInt() int {
	return 1
}

func main() {
	SaveData1("./a.txt", []byte("hello world"))
}
