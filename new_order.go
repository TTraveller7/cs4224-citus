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
	ItemId int
	Price  float64
	Name   string
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

		orderlineCmd := scanner.Text()
		orderlineWords := strings.Split(orderlineCmd, ",")
		if len(orderlineWords) < 3 {
			errMsg := fmt.Sprintf("orderline command length less than 3. lineCount=%v, orderlineCmd=%s", *lineCount, orderlineCmd)
			logs.Printf(errMsg)
			return fmt.Errorf(errMsg)
		}
		orderlineInput := &OrderlineInput{
			ItemId:    SafeParseInt(words[0]),
			SupplyWid: SafeParseInt(words[1]),
			Quantity:  SafeParseInt(words[2]),
		}
		orderlineInputs = append(orderlineInputs, orderlineInput)
	}

	isAllLocal := 1
	for _, ol := range orderlineInputs {
		if ol.SupplyWid != wid {
			isAllLocal = 0
			break
		}
	}

	// update next_o_id
	var nextOrderId int
	updateOrderIdTxn := func() error {
		return db.Transaction(func(tx *gorm.DB) error {
			if err := db.Raw(`
				SELECT d_next_o_id
				FROM district_order_id
				WHERE d_id = ? AND d_w_id = ?
				LIMIT 1
				FOR UPDATE
			`, did, wid).Row().Scan(&nextOrderId); err != nil {
				return err
			}

			err := db.Exec(`
				UPDATE district_order_id
				SET d_next_o_id = ?
				WHERE d_id = ? AND d_w_id = ?
			`, nextOrderId+1, did, wid).Error
			if err == nil {
				return err
			} else if db.RowsAffected == 0 {
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
	err := db.Raw(`
		SELECT d_tax, w_tax 
		FROM district_info 
		WHERE d_w_id = ? AND d_id = ?
		LIMIT 1
	`).Row().Scan(&dTax, &wTax)
	if err != nil {
		logs.Printf("get d_tax and w_tax failed: %v", err)
		return nil
	}

	var cDiscount float64
	var cLast, cCredit string
	err = db.Raw(`
		SELECT c_discount, c_last, c_credit
		FROM customer_info 
		WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?
		LIMIT 1
	`, wid, did, cid).Row().Scan(&cDiscount, &cLast, &cCredit)
	if err != nil {
		logs.Printf("get c_discount, c_last, c_credit failed: %v", err)
		return nil
	}

	itemIdToItemInfo := make(map[int]*ItemInfo, 0)
	for _, ol := range orderlineInputs {
		var price float64
		var name string
		err = db.Raw(`
			SELECT i_price, i_name FROM items \
			WHERE i_id = ? \
			LIMIT 1 
		`, ol.ItemId).Row().Scan(&price, &name)
		if err != nil {
			logs.Printf("get i_price. i_name failed: %v", err)
			return nil
		}
		itemInfo := &ItemInfo{
			Price: price,
			Name:  name,
		}
		itemIdToItemInfo[ol.ItemId] = itemInfo
	}

	// update all stocks
	var totalAmount float64
	orderlineOutputs := make([]*OrderlineOutput, 0)
	stockDeltas := make([]*StockDelta, 0)
	updateStockTxn := func() error {
		return db.Transaction(func(db *gorm.DB) error {
			for _, ol := range orderlineInputs {
				districtStr := strconv.FormatInt(int64(did), 10)
				if did < 10 {
					districtStr = "0" + districtStr
				}
				getStockQuery := fmt.Sprintf("SELECT s_qty, s_ytd, s_order_cnt, s_remote_cnt, s_dist_%s\nFROM stocks\nWHERE s_w_id = ? AND s_i_id = ?\nLIMIT 1\nFOR UPDATE", districtStr)

				var quantity, orderCount, remoteCount int
				var ytd float64
				var distinfo string
				err = db.Raw(getStockQuery, ol.SupplyWid, ol.ItemId).Row().Scan(&quantity, &ytd, &orderCount, &remoteCount, &distinfo)
				if err != nil {
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
				err = db.Exec(`
					UPDATE stocks
					SET s_qty = ?, s_ytd = ?, s_order_cnt = ?, s_remote_cnt = ?
					WHERE s_w_id = ? AND s_i_id = ?
				`, nextQuantity, nextYtd, nextOrderCount, nextRemoteCount, ol.SupplyWid, ol.ItemId).Error
				if err != nil {
					return err
				} else if db.RowsAffected == 0 {
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
					DistInfo:          distinfo,
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

	insertOrderTxn := func() error {
		return db.Transaction(func(db *gorm.DB) error {
			entryTime := time.Now()
			err := db.Exec(`
				INSERT INTO orders(o_w_id, o_d_id, o_id, o_c_id, o_carrier_id, o_ol_cnt, o_all_local, o_entry_d) VALUES
				(?, ?, ?, ?, NULL, ?, ?, ?)
			`, wid, did, nextOrderId, cid, numOfItems, isAllLocal, entryTime).Error
			if err != nil {
				return err
			} else if db.RowsAffected == 0 {
				return ErrNoRowsAffected
			}

			for i, ol := range orderlineOutputs {
				err = db.Exec(`
					INSERT INTO order_lines(ol_w_id, ol_d_id, ol_o_id, ol_number, ol_i_id, ol_i_name,
						ol_delivery_d, ol_amount, ol_supply_w_id, ol_quantity, ol_dist_info) VALUES
						(?, ?, ?, ?, ?, ?,
						NULL, ?, ?, ?, ?)
				`, wid, did, nextOrderId, i+1, ol.ItemId, ol.Name, ol.ItemAmount, ol.SupplyWid, ol.Quantity, ol.DistInfo).Error
				if err != nil {
					return err
				} else if db.RowsAffected == 0 {
					return ErrNoRowsAffected
				}
			}
			return nil
		})
	}
	if err := Retry(insertOrderTxn); err != nil {
		logs.Printf("insert order failed: %v", err)

		revertStockTxn := func() error {
			return db.Transaction(func(db *gorm.DB) error {
				for _, stockDelta := range stockDeltas {
					err = db.Exec(`
					UPDATE stocks 
					SET s_qty = s_qty - ?, s_ytd = s_ytd - ?, s_order_cnt = s_order_cnt - ?, s_remote_cnt = s_remote_cnt - ? 
					WHERE s_w_id = ? AND s_i_id = ?
					`, stockDelta.Quantity, stockDelta.Ytd, stockDelta.OrderCount, stockDelta.RemoteCount, stockDelta.SupplyWid, stockDelta.ItemId).Error
					if err != nil {
						return err
					} else if db.RowsAffected == 0 {
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

	return nil
}
