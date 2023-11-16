package main

import (
	"bufio"
	"fmt"
	"log"
	"strings"

	"gorm.io/gorm"
)

type CommonOrder struct {
	Wid int64
	Did int64
	Oid int64
}

func RelatedCustomer(logs *log.Logger, db *gorm.DB, words []string, scanner *bufio.Scanner, lineCount *int) error {
	wid := SafeParseInt64(words[1])
	did := SafeParseInt64(words[2])
	cid := SafeParseInt64(words[3])

	// get orders
	oids := make([]int64, 0)
	db = db.Raw(`
		select o_id 
		from orders 
		where o_w_id = %s and o_d_id = %s and o_c_id = %s
		LIMIT 10000
	`, wid, did, cid).Scan(&oids)
	if db.Error != nil {
		logs.Printf("related customer get order id failed: %v", db.Error)
		return nil
	}

	commonOrders := make([]*CommonOrder, 0)
	for _, oid := range oids {
		itemIds := make([]int64, 0)
		db = db.Raw(`
			select ol_i_id 
			from order_lines
			where ol_w_id = %s and ol_d_id = %s and ol_o_id = %s
		`, wid, did, oid).Scan(&itemIds)
		if db.Error != nil {
			logs.Printf("related customer get order line item ids failed: %v", db.Error)
			return nil
		}

		itemIdSetStr := FormatInt64Set(itemIds)
		db = db.Raw(`
			select ol_w_id, ol_d_id, ol_o_id 
			from order_lines 
			where ol_w_id != %s and ol_i_id in (%s) 
			group by ol_w_id, ol_d_id, ol_o_id 
			having count(ol_i_id) >= 2
		`, wid, itemIdSetStr)
		rows, err := db.Rows()
		if err != nil {
			logs.Printf("related customer get order lines failed: %v", err)
			return nil
		}
		for rows.Next() {
			co := &CommonOrder{}
			if err := rows.Scan(&co.Wid, &co.Did, &co.Oid); err != nil {
				logs.Printf("related customer scan order lines failed: %v", err)
				return nil
			}
			commonOrders = append(commonOrders, co)
		}
	}

	if len(commonOrders) == 0 {
		logs.Printf("There is no related customer")
		return nil
	}

	cidSet := make(map[int64]bool, 0)
	sb := strings.Builder{}
	for _, co := range commonOrders {
		var cid int64
		db = db.Raw(`
			SELECT o_c_id
			FROM orders 
			WHERE o_w_id = ? AND o_d_id = ? AND o_id = ?
			LIMIT 1
		`, co.Wid, co.Did, co.Oid)
		if err := db.Row().Scan(&cid); err != nil {
			logs.Printf("related customer scan customer failed: %v", err)
			return nil
		}

		if cidSet[cid] {
			continue
		}
		cidSet[cid] = true
		sb.WriteString(fmt.Sprintf("related customer identifier: w_id: %v, d_id: %v. c_id: %v", co.Wid, co.Did, cid))
	}
	logs.Printf(sb.String())
	return nil
}
