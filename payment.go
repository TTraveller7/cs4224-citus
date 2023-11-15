package main

import (
	"bufio"
	"fmt"
	"log"
	"strings"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

func Payment(logs *log.Logger, db *gorm.DB, words []string, scanner *bufio.Scanner, lineCount *int) error {
	wid := SafeParseInt64(words[1])
	did := SafeParseInt64(words[2])
	cid := SafeParseInt64(words[3])
	payment := SafeParseFloat64(words[4])

	var balance float64
	var paymentId string
	updateBalanceTxn := func() error {
		return db.Transaction(func(tx *gorm.DB) error {
			tx = tx.Exec(`                        
				UPDATE customer_param
				SET c_balance = c_balance - ?,
					c_ytd_payment = c_ytd_payment + ?,
					c_payment_cnt = c_payment_cnt + 1
				WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?`,
				payment, payment, wid, did, cid)
			if tx.Error != nil {
				return tx.Error
			} else if tx.RowsAffected == 0 {
				return ErrNoRowsAffected
			}

			tx = tx.Raw(`
				SELECT c_balance
				FROM customer_param
				WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?`,
				wid, did, cid)
			if err := tx.Row().Scan(&balance); err != nil {
				return err
			}

			paymentId = uuid.New().String()
			tx = tx.Exec(`
				INSERT INTO payment_history(id, w_id, d_id, c_id, amount) VALUES
				(?, ?, ?, ?, ?)
			`, paymentId, wid, did, cid, payment)
			if tx.Error != nil {
				return tx.Error
			} else if tx.RowsAffected == 0 {
				return ErrNoRowsAffected
			}

			return nil
		})
	}
	if err := Retry(updateBalanceTxn); err != nil {
		logs.Printf("update balance failed: %v", err)
		return nil
	}

	// update wytd
	updateWYtdTxn := func() error {
		return db.Transaction(func(tx *gorm.DB) error {
			tx = tx.Exec(`
				UPDATE warehouse_param
				SET w_ytd = w_ytd + ?
				WHERE w_id = ?
			`, payment, wid)
			if tx.Error != nil {
				return tx.Error
			} else if tx.RowsAffected == 0 {
				return ErrNoRowsAffected
			}

			tx = tx.Exec(`
				UPDATE payment_history
				SET is_w_ytd_updated = 1
				WHERE id = ? AND w_id = ? AND d_id = ? AND c_id = ?
			`, paymentId, wid, did, cid)
			if tx.Error != nil {
				return tx.Error
			} else if tx.RowsAffected == 0 {
				return ErrNoRowsAffected
			}

			return nil
		})
	}
	if err := Retry(updateWYtdTxn); err != nil {
		logs.Printf("update w_ytd failed: %v", err)
		return nil
	}

	// update dytd
	updateDYtdTxn := func() error {
		return db.Transaction(func(tx *gorm.DB) error {
			tx = tx.Exec(`
				UPDATE district_param
				SET d_ytd = d_ytd + ?
				WHERE d_w_id = ? AND d_id = ?
			`, payment, wid, did)
			if tx.Error != nil {
				return tx.Error
			} else if tx.RowsAffected == 0 {
				return ErrNoRowsAffected
			}

			tx = tx.Exec(`
				UPDATE payment_history
				SET is_d_ytd_updated = 1
				WHERE id = ? AND w_id = ? AND d_id = ? AND c_id = ?
			`, paymentId, wid, did, cid)
			if tx.Error != nil {
				return tx.Error
			} else if tx.RowsAffected == 0 {
				return ErrNoRowsAffected
			}

			return nil
		})
	}
	if err := Retry(updateDYtdTxn); err != nil {
		logs.Printf("update d_ytd failed: %v", err)
		return nil
	}

	ci := CustomerInfo{}
	db = db.Raw(`
		SELECT c_w_id, c_d_id, c_id, c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_since, c_credit, c_credit_lim, c_discount, c_data
		FROM customer_info
		WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?
		LIMIT 1
	`, wid, did, cid)
	if err := db.Row().Scan(&ci); err != nil {
		logs.Printf("get customer_info failed: %v", err)
		return nil
	}

	di := DistrictInfo{}
	db = db.Raw(`
		SELECT d_id, d_w_id, w_name, w_street_1, w_street_2, w_city, w_state, w_zip, w_tax, d_name, d_street_1, d_street_2, d_city, d_state, d_zip, d_tax 
		FROM district_info
		WHERE d_w_id = ? AND d_id = ?
		LIMIT 1
	`, wid, did)
	if err := db.Row().Scan(&di); err != nil {
		logs.Printf("get district info failed: %v", err)
		return nil
	}

	sb := strings.Builder{}

	sb.WriteString(fmt.Sprintf("c_w_id: %v, c_d_id: %v, c_id: %v, c_first: %s, c_middle: %s, c_last: %s, ", ci.CWId, ci.CDId, ci.CId, ci.CFirst, ci.CMiddle, ci.CLast))
	sb.WriteString(fmt.Sprintf("c_street_1: %s, c_street_2: %s, c_city: %s, c_state: %s, c_zip: %s, ", ci.CStreet1, ci.CStreet2, ci.CCity, ci.CState, ci.CZip))
	sb.WriteString(fmt.Sprintf("c_phone: %s, c_since: %v, c_credit: %s, c_credit_lim: %v, c_discount: %v, c_balance: %v\n", ci.CPhone, ci.CSince, ci.CCredit, ci.CCreditLim, ci.CDiscount, balance))

	sb.WriteString(fmt.Sprintf("w_street_1: %s, w_street_2: %s, w_city: %s, w_state: %s, w_zip: %s\n", di.WStreet1, di.WStreet2, di.WCity, di.WState, di.WZip))
	sb.WriteString(fmt.Sprintf("d_street_1: %s, d_street_2: %s, d_city: %s, d_state: %s, d_zip: %s\n", di.DStreet1, di.DStreet2, di.DCity, di.DState, di.DZip))
	sb.WriteString(fmt.Sprintf("payment: %v", payment))
	logs.Printf(sb.String())

	return nil
}
