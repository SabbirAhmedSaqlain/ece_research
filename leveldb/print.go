package main

import (
	"fmt"
	"log"

	_ "github.com/go-sql-driver/mysql"
	"github.com/syndtr/goleveldb/leveldb"
)

func main() {

	// LevelDB connection details
	databaseName := "enc_database1m1"
	// fmt.Printf("\n\n Enter database name: ")
	// _, _ = fmt.Scanf("%d", &databaseName)

	levelDB, err := leveldb.OpenFile(databaseName, nil)
	if err != nil {
		log.Fatal("Failed to open LevelDB:", err)
	}
	defer levelDB.Close()

	//--------------------print leveldb test data--------------

	exp_type := 0
	fmt.Printf("\n\n Enter 0 to print 5 data else to data count: ")
	_, err = fmt.Scanf("%d", &exp_type)

	if exp_type == 0 {

		fmt.Printf("\n\n Enter amount to print: ")
		_, err = fmt.Scanf("%d", &exp_type)

		ct := 0
		iter := levelDB.NewIterator(nil, nil)
		for iter.Next() {
			key := iter.Key()
			value := iter.Value()
			ct += 1
			fmt.Printf("\n\nKey: %s, Value: %s\n", key, value)
			if ct == exp_type {
				break
			}

		}
	} else {
		ct := 0
		iter := levelDB.NewIterator(nil, nil)
		for iter.Next() {

			ct += 1

		}

		fmt.Printf("\n\nTotal data: %v\n", ct)
	}

}
