package main

import (
	"bufio"
	"fmt"
	"log"
	"strings"
	"time"

	"gorm.io/gorm"
)

type OrderOutput struct {
	OrderId             int64
	EntryDate           time.Time
	Cid                 int64
	CFirst              string
	CMiddle             string
	CLast               string
	PopularItemIds      []int64
	PopularItemQuantity int64
}

type PopularItemOrderline struct {
	OrderId  int64
	ItemId   int64
	Quantity int64
}

func PopularItem(logs *log.Logger, db *gorm.DB, words []string, scanner *bufio.Scanner, lineCount *int) error {
	wid := SafeParseInt64(words[1])
	did := SafeParseInt64(words[2])
	l := SafeParseInt64(words[3])

	var nextOrderId int64
	getOrderIdTxn := func() error {
		return db.Transaction(func(tx *gorm.DB) error {
			tx = tx.Raw(`
				select d_next_o_id 
				from district_order_id 
				where d_w_id=? and d_id=?
			`, wid, did)
			if err := tx.Row().Scan(&nextOrderId); err != nil {
				return err
			}
			return nil
		})
	}
	if err := Retry(getOrderIdTxn); err != nil {
		logs.Printf("popular item get order id failed: %v", err)
		return nil
	}
	orderIdStart := nextOrderId - l

	orderOutputs := make([]*OrderOutput, 0)
	orderlines := make([]*PopularItemOrderline, 0)
	cids := make([]int64, 0)
	itemIdSet := make(map[int64]bool, 0)
	getOrderAndOrderlineTxn := func() error {
		return db.Transaction(func(tx *gorm.DB) error {
			tx = tx.Raw(`
				select o_id, o_entry_d, o_c_id 
				from orders 
				where o_w_id=? and o_d_id=? and o_id >= ?
			`, wid, did, orderIdStart)
			rows, err := tx.Rows()
			if err != nil {
				return err
			}
			for rows.Next() {
				o := &OrderOutput{}
				if err := rows.Scan(&o.OrderId, &o.EntryDate, &o.Cid); err != nil {
					return err
				}
				orderOutputs = append(orderOutputs, o)
				cids = append(cids, o.Cid)
			}

			tx = tx.Raw(`
				select ol_o_id, ol_i_id, ol_quantity 
				from order_lines 
				where ol_w_id = ? and ol_d_id = ? and ol_o_id >= ?
			`, wid, did, orderIdStart)
			rows, err = tx.Rows()
			if err != nil {
				return err
			}
			for rows.Next() {
				ol := &PopularItemOrderline{}
				if err := rows.Scan(&ol.OrderId, &ol.ItemId, &ol.Quantity); err != nil {
					return err
				}
				orderlines = append(orderlines, ol)
				itemIdSet[ol.ItemId] = true
			}
			return nil
		})
	}
	if err := Retry(getOrderAndOrderlineTxn); err != nil {
		logs.Printf("get orders and orderlines failed: %v", err)
		return nil
	}

	itemIds := make([]int64, 0, len(itemIdSet))
	for itemId := range itemIdSet {
		itemIds = append(itemIds, itemId)
	}

	for _, o := range orderOutputs {
		db = db.Raw(`
			SELECT c_first, c_middle, c_last
			FROM customer_info
			WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?
			LIMIT 1
		`, wid, did, o.Cid)
		if err := db.Row().Scan(&o.CFirst, &o.CMiddle, &o.CLast); err != nil {
			logs.Printf("popular item get customer name failed: %v", err)
			return nil
		}
	}

	itemIdToItemName := make(map[int64]string, 0)
	db = db.Raw(`
		select i_id, i_name 
		from items 
		where i_id in ?
	`, itemIds)
	rows, err := db.Rows()
	if err != nil {
		logs.Printf("popular item get item names failed: %v", err)
		return nil
	}
	for rows.Next() {
		var id int64
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			logs.Printf("popular item scan item names failed: %v", err)
			return nil
		}
		itemIdToItemName[id] = name
	}

	popularItemIdtoCount := make(map[int64]int64, 0)

	for _, o := range orderOutputs {
		var maxQuantity int64 = 0
		popularItemIds := make([]int64, 0)
		for _, ol := range orderlines {
			if ol.OrderId != o.OrderId {
				continue
			}
			if ol.Quantity > maxQuantity {
				popularItemIds = []int64{ol.ItemId}
				maxQuantity = ol.Quantity
			} else if ol.Quantity == maxQuantity {
				popularItemIds = append(popularItemIds, ol.ItemId)
			}
		}

		o.PopularItemQuantity = maxQuantity
		o.PopularItemIds = popularItemIds
		for _, itemId := range popularItemIds {
			popularItemIdtoCount[itemId] = popularItemIdtoCount[itemId] + 1
		}
	}

	sb := strings.Builder{}
	sb.WriteString(fmt.Sprintf("w_id: %v, d_id: %v\n", wid, did))
	sb.WriteString(fmt.Sprintf("L: %v\n", l))
	for _, o := range orderOutputs {
		sb.WriteString(fmt.Sprintf("o_id: %v, o_entry_d: %v\n", o.OrderId, o.EntryDate))
		sb.WriteString(fmt.Sprintf("c_first: %s, c_middle: %s, c_last: %s\n", o.CFirst, o.CMiddle, o.CLast))
		for _, itemId := range o.PopularItemIds {
			sb.WriteString(fmt.Sprintf("i_name: %s, quantity: %v\n", itemIdToItemName[itemId], o.PopularItemQuantity))
		}
	}
	total := float64(l)
	for itemId, count := range popularItemIdtoCount {
		sb.WriteString(fmt.Sprintf("i_name: %s, percentage of orders in S that contain the popular item: %v", itemIdToItemName[itemId], float64(count)/total*100.0))
		sb.WriteString("\n")
	}
	logs.Printf(sb.String())

	return nil
}
