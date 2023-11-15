package main

import (
	"bufio"
	"log"
	"time"

	"gorm.io/gorm"
)

func Delivery(logs *log.Logger, db *gorm.DB, words []string, scanner *bufio.Scanner, lineCount *int) error {
	wid := SafeParseInt64(words[1])
	carrierId := SafeParseInt64(words[2])

	dids := make([]int64, 0)
	db = db.Raw(`
		SELECT d_id 
		FROM district_info
		WHERE d_w_id = ?
		LIMIT 10000
	`, wid).Scan(&dids)
	if db.Error != nil {
		logs.Printf("get all d_id failed: %v", db.Error)
		return nil
	}

	for _, did := range dids {
		var maxOrderId int64
		getOidTxn := func() error {
			return db.Transaction(func(tx *gorm.DB) error {
				tx = tx.Raw(`
					SELECT d_next_o_id
					FROM district_order_id
					WHERE d_w_id = ? AND d_id = ?
					LIMIT 1
				`, wid, did)
				if err := tx.Row().Scan(&maxOrderId); err != nil {
					return err
				}
				return nil
			})
		}
		if err := Retry(getOidTxn); err != nil {
			logs.Printf("get max oid failed: %v", err)
			continue
		}

		var oid int64
		getOidPtrTxn := func() error {
			return db.Transaction(func(tx *gorm.DB) error {
				tx = tx.Raw(`
                    SELECT next_delivery_o_id
                    FROM delivery_cursor
                    WHERE w_id = ? AND d_id = ?
                    LIMIT 1
				`, wid, did)
				if err := tx.Row().Scan(&oid); err != nil {
					return err
				}
				return nil
			})
		}
		if err := Retry(getOidPtrTxn); err != nil {
			logs.Printf("get oid pointer failed: %v", err)
			continue
		}

		for oidPtr := oid; oidPtr < maxOrderId; oidPtr++ {
			var cid int64
			db = db.Raw(`
                SELECT o_c_id
                FROM orders
                WHERE o_w_id = ? AND o_d_id = ? AND o_id = ?
            `, wid, did, oidPtr)
			if err := db.Row().Scan(&cid); err != nil {
				return err
			}

			updated := false
			deliverToDistrictTxn := func() error {
				return db.Transaction(func(tx *gorm.DB) error {
					var isCarrierIdNull int64
					tx = tx.Raw(`
                        SELECT COALESCE(o_carrier_id, -1)
                        FROM orders
                        WHERE o_w_id = ? AND o_d_id = ? AND o_id = ?
                        LIMIT 1 
                        FOR UPDATE
                    `, wid, did, oidPtr)
					if err := tx.Row().Scan(&isCarrierIdNull); err != nil {
						return err
					}
					if isCarrierIdNull != -1 {
						return nil
					}

					tx = tx.Exec(`
                        UPDATE orders
                        SET o_carrier_id = ?
                        WHERE o_w_id = ? AND o_d_id = ? AND o_id = ?
                    `, carrierId, wid, did, oidPtr)
					if tx.Error != nil {
						return tx.Error
					} else if tx.RowsAffected == 0 {
						return ErrNoRowsAffected
					}

					tx = tx.Exec(`
                        UPDATE delivery_cursor
                        SET next_delivery_o_id = ?
                        WHERE w_id = ? AND d_id = ? AND next_delivery_o_id < ?
                    `, oidPtr+1, wid, did, oidPtr+1)
					if tx.Error != nil {
						return tx.Error
					}

					now := time.Now().UTC()
					tx = tx.Exec(`
                        UPDATE order_lines
                        SET ol_delivery_d = ?
                        WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ?
                    `, now, wid, did, oidPtr)
					if tx.Error != nil {
						return tx.Error
					} else if tx.RowsAffected == 0 {
						return ErrNoRowsAffected
					}

					olAmounts := make([]float64, 0)
					tx = tx.Raw(`
                        SELECT ol_amount
                        FROM order_lines
                        WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ?
                    `, wid, did, oidPtr).Scan(&olAmounts)
					if tx.Error != nil {
						return tx.Error
					}

					var sum float64 = 0.0
					for _, olAmount := range olAmounts {
						sum += olAmount
					}

					tx = tx.Raw(`
                        UPDATE customer_param
                        SET c_balance = c_balance + ?, c_delivery_cnt = c_delivery_cnt + 1
                        WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?
                    `, sum, wid, did, cid)
					if tx.Error != nil {
						return tx.Error
					} else if tx.RowsAffected == 0 {
						return ErrNoRowsAffected
					}
					updated = true
					return nil
				})
			}
			if err := Retry(deliverToDistrictTxn); err != nil {
				logs.Printf("deliver to district failed: %v", err)
				continue
			}
			if updated {
				break
			}
		}
	}

	return nil
}
