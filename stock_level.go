package main

import (
	"bufio"
	"log"

	"gorm.io/gorm"
)

func StockLevel(logs *log.Logger, db *gorm.DB, words []string, scanner *bufio.Scanner, lineCount *int) error {
	wid := SafeParseInt64(words[1])
	did := SafeParseInt64(words[2])
	t := SafeParseInt64(words[3])
	l := SafeParseInt64(words[4])

	var count int64
	getStocksTxn := func() error {
		return db.Transaction(func(tx *gorm.DB) error {
			var nextOrderId int64
			tx = tx.Raw(`
				select d_next_o_id
				from district_order_id
				where d_w_id=? and d_id=?
			`, wid, did)
			if err := tx.Row().Scan(&nextOrderId); err != nil {
				return err
			}

			lStart := nextOrderId - l
			lEnd := nextOrderId - 1
			itemIds := make([]int64, 0)
			tx = tx.Raw(`
				select ol_i_id 
				from order_lines
				where ol_w_id=? and ol_d_id = ? and ol_o_id between ? and ?
			`, wid, did, lStart, lEnd).Scan(&itemIds)
			if tx.Error != nil {
				return tx.Error
			}

			tx = tx.Raw(`
				select count(*) 
				from stocks 
				where s_w_id=? and s_qty < ? and s_i_id in ?
			`, wid, t, itemIds)
			if err := tx.Row().Scan(&count); err != nil {
				return err
			}

			return nil
		})
	}
	if err := Retry(getStocksTxn); err != nil {
		logs.Printf("get stock level failed: %v", err)
		return nil
	}

	logs.Printf("Total number of items with stock quantity less than threshold: %v", count)

	return nil
}
