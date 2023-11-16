package main

import (
	"bufio"
	"fmt"
	"log"
	"strings"
	"time"

	"gorm.io/gorm"
)

type OrderlineInfo struct {
	ItemId       int64
	SupplyWid    int64
	Quantity     int64
	Amount       float64
	DeliveryDate time.Time
}

func OrderStatus(logs *log.Logger, db *gorm.DB, words []string, scanner *bufio.Scanner, lineCount *int) error {
	wid := SafeParseInt64(words[1])
	did := SafeParseInt64(words[2])
	cid := SafeParseInt64(words[3])

	var cFirst, cMiddle, cLast string
	db = db.Raw(`
		SELECT c_first, c_middle, c_last
		FROM customer_info
		WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?
		LIMIT 1
	`, wid, did, cid)
	if err := db.Row().Scan(&cFirst, &cMiddle, &cLast); err != nil {
		logs.Printf("get customer name failed: %v", err)
		return nil
	}

	var balance float64
	var carrierId, lastOrderId, olCount int64
	var entryDate time.Time
	orderlineInfos := make([]*OrderlineInfo, 0)
	getLastOrderTxn := func() error {
		return db.Transaction(func(tx *gorm.DB) error {
			tx = tx.Raw(`
				SELECT c_balance, c_last_o_id
				FROM customer_param
				WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?
				LIMIT 1
			`, wid, did, cid)
			if err := tx.Row().Scan(&balance, &lastOrderId); err != nil {
				return err
			}

			tx = tx.Raw(`
				SELECT COALESCE(o_carrier_id, -1), o_ol_cnt, o_entry_d
				FROM orders
				WHERE o_w_id = ? AND o_d_id = ? AND o_id = ?
				LIMIT 1
			`, wid, did, lastOrderId)
			if err := tx.Row().Scan(&carrierId, &olCount, &entryDate); err != nil {
				return err
			}

			tx = tx.Raw(`
				SELECT ol_i_id, COALESCE(ol_delivery_d, '0001-01-01 00:00:00'), ol_amount, ol_supply_w_id, ol_quantity
				FROM order_lines
				WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ?
				LIMIT ?
			`, wid, did, lastOrderId, olCount)
			rows, err := tx.Rows()
			if err != nil {
				return err
			}
			for rows.Next() {
				ol := &OrderlineInfo{}
				if err := rows.Scan(&ol.ItemId, &ol.DeliveryDate, &ol.Amount, &ol.SupplyWid, &ol.Quantity); err != nil {
					return err
				}
				orderlineInfos = append(orderlineInfos, ol)
			}
			return nil
		})
	}
	if err := Retry(getLastOrderTxn); err != nil {
		logs.Printf("get last order failed: %v", err)
		return nil
	}
	carrierIdStr := ""
	if carrierId != -1 {
		carrierIdStr = fmt.Sprintf("%v", carrierId)
	}

	sb := strings.Builder{}
	sb.WriteString(fmt.Sprintf("first name: %s, middle name: %s, last name: %s\n", cFirst, cMiddle, cLast))
	sb.WriteString(fmt.Sprintf("balance: %v\n", balance))
	sb.WriteString(fmt.Sprintf("o_id: %v, o_entry_d: %v, o_carrier_id: %s\n", lastOrderId, entryDate, carrierIdStr))
	for _, ol := range orderlineInfos {
		deliveryDateStr := fmt.Sprintf("%v", ol.DeliveryDate)
		if ol.DeliveryDate.IsZero() {
			deliveryDateStr = ""
		}
		sb.WriteString(fmt.Sprintf("ol_i_id: %v, ol_supply_w_id: %v, ol_quantity: %v, ol_amount: %v, ol_delivery_d: %s\n", ol.ItemId, ol.SupplyWid, ol.Quantity, ol.Amount, deliveryDateStr))
	}
	logs.Printf(sb.String())
	return nil
}
