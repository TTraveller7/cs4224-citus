package main

import (
	"bufio"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"gorm.io/gorm"
)

type OrderlineInput struct {
	ItemId    int
	SupplyWid int
	Quantity  int
}

type ItemInfo struct {
	ItemId   int
	Price    float64
	Name     string
	DistInfo string
}

// ol_i_id, i_name, ol_supply_w_id, ol_quantity, item_amount, next_qty
type OrderlineOutput struct {
	ItemId            int
	Name              string
	SupplyWid         int
	OrderlineQuantity int
	ItemAmount        float64
	Quantity          int
	DistInfo          string
}

type StockDelta struct {
	Quantity    int
	Ytd         float64
	OrderCount  int
	RemoteCount int
	SupplyWid   int
	ItemId      int
}

func NewOrder(logs *log.Logger, db *gorm.DB, words []string, scanner *bufio.Scanner, lineCount *int) error {
	cid := SafeParseInt(words[1])
	wid := SafeParseInt(words[2])
	did := SafeParseInt(words[3])
	numOfItems := SafeParseInt(words[4])

	orderlineInputs := make([]*OrderlineInput, 0, numOfItems)
	for i := 0; i < int(numOfItems); i++ {
		if !scanner.Scan() {
			errMsg := fmt.Sprintf("unexpected EOF. lineCount=%v", *lineCount)
			logs.Printf(errMsg)
			return fmt.Errorf(errMsg)
		}

		*lineCount++
		orderlineCmd := scanner.Text()
		orderlineWords := strings.Split(orderlineCmd, ",")
		if len(orderlineWords) < 3 {
			errMsg := fmt.Sprintf("orderline command length less than 3. lineCount=%v, orderlineCmd=%s", *lineCount, orderlineCmd)
			logs.Printf(errMsg)
			return fmt.Errorf(errMsg)
		}
		orderlineInput := &OrderlineInput{
			ItemId:    SafeParseInt(orderlineWords[0]),
			SupplyWid: SafeParseInt(orderlineWords[1]),
			Quantity:  SafeParseInt(orderlineWords[2]),
		}
		orderlineInputs = append(orderlineInputs, orderlineInput)
	}

	isAllLocal := true
	for _, ol := range orderlineInputs {
		if ol.SupplyWid != wid {
			isAllLocal = false
			break
		}
	}

	// update next_o_id
	var nextOrderId int
	updateOrderIdTxn := func() error {
		return db.Transaction(func(tx *gorm.DB) error {
			tx = tx.Raw(`
				SELECT d_next_o_id
				FROM district_order_id
				WHERE d_id = ? AND d_w_id = ?
				LIMIT 1`, did, wid)
			if err := tx.Row().Scan(&nextOrderId); err != nil {
				return err
			}

			tx = tx.Exec(`
				UPDATE district_order_id
				SET d_next_o_id = ?
				WHERE d_id = ? AND d_w_id = ?
			`, nextOrderId+1, did, wid)
			if tx.Error != nil {
				return tx.Error
			} else if tx.RowsAffected == 0 {
				return ErrNoRowsAffected
			}
			return nil
		})
	}
	if err := Retry(updateOrderIdTxn); err != nil {
		logs.Printf("update next_o_id failed: %v", err)
		return nil
	}

	var dTax, wTax float64
	db = db.Raw(`
		SELECT d_tax, w_tax 
		FROM district_info 
		WHERE d_w_id = ? AND d_id = ?
		LIMIT 1
	`, wid, did)
	if err := db.Row().Scan(&dTax, &wTax); err != nil {
		logs.Printf("get d_tax and w_tax failed: %v", err)
		return nil
	}

	var cDiscount float64
	var cLast, cCredit string
	db = db.Raw(`
		SELECT c_discount, c_last, c_credit
		FROM customer_info 
		WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?
		LIMIT 1
	`, wid, did, cid)
	if err := db.Row().Scan(&cDiscount, &cLast, &cCredit); err != nil {
		logs.Printf("get c_discount, c_last, c_credit failed: %v", err)
		return nil
	}

	itemIdToItemInfo := make(map[int]*ItemInfo, 0)
	for _, ol := range orderlineInputs {
		var price float64
		var name string
		db = db.Raw(`
			SELECT i_price, i_name FROM items
			WHERE i_id = ? 
			LIMIT 1 
		`, ol.ItemId)
		if err := db.Row().Scan(&price, &name); err != nil {
			logs.Printf("get i_price, i_name failed: %v", err)
			return nil
		}

		districtStr := strconv.FormatInt(int64(did), 10)
		if did < 10 {
			districtStr = "0" + districtStr
		}
		q := fmt.Sprintf("SELECT s_dist_%s FROM stock_info_by_district WHERE s_w_id = ? AND s_i_id = ? LIMIT 1", districtStr)
		var distInfo string
		db = db.Raw(q, wid, ol.ItemId)
		if err := db.Row().Scan(&distInfo); err != nil {
			logs.Printf("get dist_info failed: %v", err)
			return nil
		}

		itemInfo := &ItemInfo{
			Price:    price,
			Name:     name,
			DistInfo: distInfo,
		}
		itemIdToItemInfo[ol.ItemId] = itemInfo
	}

	// update all stocks
	var totalAmount float64
	orderlineOutputs := make([]*OrderlineOutput, 0)
	stockDeltas := make([]*StockDelta, 0)
	updateStockTxn := func() error {
		return db.Transaction(func(tx *gorm.DB) error {
			for _, ol := range orderlineInputs {
				var quantity, orderCount, remoteCount int
				var ytd float64
				tx = tx.Raw(`
					SELECT s_qty, s_ytd, s_order_cnt, s_remote_cnt
					FROM stocks 
					WHERE s_w_id = ? AND s_i_id = ? 
					LIMIT 1`, ol.SupplyWid, ol.ItemId)
				if err := tx.Row().Scan(&quantity, &ytd, &orderCount, &remoteCount); err != nil {
					return err
				}
				nextQuantity := ol.Quantity + quantity
				if nextQuantity < 10 {
					nextQuantity += 100
				}
				nextYtd := ytd + float64(ol.Quantity)
				nextOrderCount := orderCount + 1
				nextRemoteCount := remoteCount
				if ol.SupplyWid != wid {
					nextRemoteCount++
				}
				tx = tx.Exec(`
					UPDATE stocks
					SET s_qty = ?, s_ytd = ?, s_order_cnt = ?, s_remote_cnt = ?
					WHERE s_w_id = ? AND s_i_id = ?
				`, nextQuantity, nextYtd, nextOrderCount, nextRemoteCount, ol.SupplyWid, ol.ItemId)
				if tx.Error != nil {
					return tx.Error
				} else if tx.RowsAffected == 0 {
					return ErrNoRowsAffected
				}
				itemInfo := itemIdToItemInfo[ol.ItemId]
				itemAmount := itemInfo.Price * float64(ol.Quantity)
				totalAmount += itemAmount

				orderlineOutput := &OrderlineOutput{
					ItemId:            ol.ItemId,
					Name:              itemInfo.Name,
					SupplyWid:         ol.SupplyWid,
					OrderlineQuantity: ol.Quantity,
					ItemAmount:        itemAmount,
					Quantity:          nextQuantity,
					DistInfo:          itemInfo.DistInfo,
				}
				orderlineOutputs = append(orderlineOutputs, orderlineOutput)

				stockDelta := &StockDelta{
					Quantity:    nextQuantity - quantity,
					Ytd:         nextYtd - ytd,
					OrderCount:  1,
					RemoteCount: nextRemoteCount - remoteCount,
					SupplyWid:   ol.SupplyWid,
					ItemId:      ol.ItemId,
				}
				stockDeltas = append(stockDeltas, stockDelta)
			}
			return nil
		})
	}
	if err := Retry(updateStockTxn); err != nil {
		logs.Printf("update stocks failed: %v", err)
		return nil
	}

	entryTime := time.Now().UTC()
	insertOrderTxn := func() error {
		return db.Transaction(func(tx *gorm.DB) error {
			tx = tx.Exec(`
				INSERT INTO orders(o_w_id, o_d_id, o_id, o_c_id, o_carrier_id, o_ol_cnt, o_all_local, o_entry_d) VALUES
				(?, ?, ?, ?, NULL, ?, ?, ?)
			`, wid, did, nextOrderId, cid, numOfItems, isAllLocal, entryTime)
			if tx.Error != nil {
				return tx.Error
			} else if tx.RowsAffected == 0 {
				return ErrNoRowsAffected
			}

			tx = tx.Exec(`
				UPDATE customer_param
				SET c_last_o_id = ?
				WHERE c_w_id = ? AND c_d_id = ? AND c_id = ? AND c_last_o_id < ?
			`, nextOrderId, wid, did, cid, nextOrderId)
			if tx.Error != nil {
				return tx.Error
			}

			for i, ol := range orderlineOutputs {
				tx = tx.Exec(`
					INSERT INTO order_lines(ol_w_id, ol_d_id, ol_o_id, ol_number, ol_i_id, ol_i_name,
						ol_delivery_d, ol_amount, ol_supply_w_id, ol_quantity, ol_dist_info) VALUES
						(?, ?, ?, ?, ?, ?,
						NULL, ?, ?, ?, ?)
				`, wid, did, nextOrderId, i+1, ol.ItemId, ol.Name, ol.ItemAmount, ol.SupplyWid, ol.Quantity, ol.DistInfo)
				if tx.Error != nil {
					return tx.Error
				} else if tx.RowsAffected == 0 {
					return ErrNoRowsAffected
				}
			}
			return nil
		})
	}
	if err := Retry(insertOrderTxn); err != nil {
		logs.Printf("insert order failed: %v", err)

		revertStockTxn := func() error {
			return db.Transaction(func(tx *gorm.DB) error {
				for _, stockDelta := range stockDeltas {
					tx = tx.Exec(`
					UPDATE stocks 
					SET s_qty = s_qty - ?, s_ytd = s_ytd - ?, s_order_cnt = s_order_cnt - ?, s_remote_cnt = s_remote_cnt - ? 
					WHERE s_w_id = ? AND s_i_id = ?
					`, stockDelta.Quantity, stockDelta.Ytd, stockDelta.OrderCount, stockDelta.RemoteCount, stockDelta.SupplyWid, stockDelta.ItemId)
					if tx.Error != nil {
						return tx.Error
					} else if tx.RowsAffected == 0 {
						return ErrNoRowsAffected
					}
				}
				return nil
			})
		}
		if err := Retry(revertStockTxn); err != nil {
			logs.Printf("revert stock failed: %v", err)
			return nil
		}

		return nil
	}

	sb := strings.Builder{}
	sb.WriteString(fmt.Sprintf("c_w_id: %v, c_d_id: %v, c_id: %v, c_last: %v, c_credit: %v, c_discount: %v\n", wid, did, cid, cLast, cCredit, cDiscount))
	sb.WriteString(fmt.Sprintf("o_id: %v, o_entry_d: %v\n", nextOrderId, entryTime))
	sb.WriteString(fmt.Sprintf("num_items: %v, total_amount: %v\n", numOfItems, totalAmount))
	for _, ol := range orderlineOutputs {
		sb.WriteString(fmt.Sprintf("item_numer: %v, i_name: %v, supplier_warehouse: %v, quantity: %v, ol_amount: %v, s_quantity: %v\n", ol.ItemId, ol.Name, ol.SupplyWid, ol.OrderlineQuantity, ol.ItemAmount, ol.Quantity))
	}
	logs.Printf(sb.String())
	return nil
}
