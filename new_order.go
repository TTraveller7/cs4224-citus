package main

import (
	"bufio"
	"fmt"
	"log"
	"strings"

	"gorm.io/gorm"
)

type OrderlineInput struct {
	ItemId    int
	SupplyWid int
	Quantity  int
}

func NewOrder(logs *log.Logger, db *gorm.DB, words []string, scanner *bufio.Scanner, lineCount *int) error {
	_ = SafeParseInt(words[0])
	wid := SafeParseInt(words[1])
	did := SafeParseInt(words[2])
	numOfItems := SafeParseInt(words[3])

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

	// update next_o_id
	var nextOrderId int
	updateOrderIdTxn := func() error {
		return db.Transaction(func(tx *gorm.DB) error {
			if err := db.Raw(`
				SELECT d_next_o_id
				FROM district_order_id
				WHERE d_id = %s AND d_w_id = %s
				LIMIT 1
				FOR UPDATE
			`, did, wid).Row().Scan(&nextOrderId); err != nil {
				return err
			}

			err := db.Exec(`
				UPDATE district_order_id
				SET d_next_o_id = %s
				WHERE d_id = %s AND d_w_id = %s
			`, nextOrderId+1, did, wid).Error
			if err == nil {
				return err
			} else if db.RowsAffected == 0 {
				return fmt.Errorf("update order id affected 0 rows")
			}
			return nil
		})
	}
	if err := Retry(updateOrderIdTxn); err != nil {
		logs.Printf("update next_o_id: %v", err)
		return nil
	}

	return nil
}
