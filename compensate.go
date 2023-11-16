package main

import (
	"context"
	"log"
	"os"
	"time"

	"gorm.io/gorm"
)

type PaymentPointer struct {
	Wid     int64
	Did     int64
	Pointer time.Time
}

type PaymentHistory struct {
	PaymentId     string
	Wid           int64
	Did           int64
	Cid           int64
	Amount        float64
	IsWYtdUpdated int64
	IsDYtdUpdated int64
	CreatedAt     time.Time
}

func Compensate(ctx context.Context, db *gorm.DB) {
	logs := log.New(os.Stdout, "[compensate] ", 0)
	logs.Printf("starts")

	defer func() {
		if err := recover(); err != nil {
			logs.Printf("recovers from panic. err: \n%v", err)
		}
	}()
	compensate(ctx, logs, db)
}

func compensate(ctx context.Context, log *log.Logger, db *gorm.DB) {
	lastUpdated := time.Now().UTC()

	for time.Since(lastUpdated) <= 5*time.Minute {
		select {
		case <-ctx.Done():
			logs.Printf("cancelled by parent")
			return
		default:

		}

		t, err := doCompensate(ctx, log, db, lastUpdated)
		if err != nil {
			logs.Printf("do compensate failed: %v", err)
		} else {
			lastUpdated = t
		}
		time.Sleep(10 * time.Second)
	}
}

func doCompensate(ctx context.Context, log *log.Logger, db *gorm.DB, lastUpdated time.Time) (time.Time, error) {
	paymentPointers := make([]*PaymentPointer, 0)
	db = db.Raw(`
		SELECT *
		FROM payment_pointer
		LIMIT 10000
	`)
	rows, err := db.Rows()
	if err != nil {
		logs.Printf("get payment_pointer failed: %v", err)
		return lastUpdated, err
	}
	for rows.Next() {
		ptr := &PaymentPointer{}
		if err := rows.Scan(&ptr.Wid, &ptr.Did, &ptr.Pointer); err != nil {
			logs.Printf("scan payment pointer failed: %v", err)
			return lastUpdated, err
		}
		paymentPointers = append(paymentPointers, ptr)
	}

	nextLastUpdated := lastUpdated
	for _, ptr := range paymentPointers {
		hasUpdate := false
		var deltaWYtd float64 = 0.0
		var deltaDYtd float64 = 0.0
		compensateTxn := func() error {
			return db.Transaction(func(tx *gorm.DB) error {
				tx = tx.Raw(`
					SELECT *
					FROM payment_history
					WHERE w_id = ? AND d_id = ? AND created_at > ?
					ORDER BY created_at
					LIMIT 100
				`, ptr.Wid, ptr.Did, ptr.Pointer)
				rows, err := tx.Rows()
				if err != nil {
					return err
				}
				hists := make([]*PaymentHistory, 0)
				for rows.Next() {
					h := &PaymentHistory{}
					if err := rows.Scan(&h.PaymentId, &h.Wid, &h.Did, &h.Cid, &h.Amount, &h.IsWYtdUpdated, &h.IsDYtdUpdated, &h.CreatedAt); err != nil {
						return err
					}
					hists = append(hists, h)
				}

				if len(hists) == 0 {
					return nil
				}

				deltaWYtd = 0.0
				deltaDYtd = 0.0
				for _, h := range hists {
					if h.IsWYtdUpdated == 0 {
						deltaWYtd += h.Amount
					}
					if h.IsDYtdUpdated == 0 {
						deltaDYtd += h.Amount
					}
				}

				if deltaWYtd > 0 {
					tx = tx.Exec(`
						UPDATE warehouse_param
						SET w_ytd = w_ytd + ?
						WHERE w_id = ?
					`, deltaWYtd, ptr.Wid)
					if tx.Error != nil {
						return err
					} else if tx.RowsAffected == 0 {
						return ErrNoRowsAffected
					}
				}

				if deltaDYtd > 0 {
					tx = tx.Exec(`
						UPDATE district_param
						SET d_ytd = d_ytd + ?
						WHERE d_w_id = ? AND d_id = ?
					`, deltaDYtd, ptr.Wid, ptr.Did)
					if tx.Error != nil {
						return err
					} else if tx.RowsAffected == 0 {
						return ErrNoRowsAffected
					}
				}

				maxCreatedAt := hists[0].CreatedAt
				for _, h := range hists {
					if h.IsDYtdUpdated == 0 || h.IsWYtdUpdated == 0 {
						tx = tx.Exec(`
							UPDATE payment_history
							SET is_w_ytd_updated = 1, is_d_ytd_updated = 1
							WHERE w_id = ? AND d_id = ? AND id = ?
						`, h.Wid, h.Did, h.PaymentId)
						if tx.Error != nil {
							return err
						} else if tx.RowsAffected == 0 {
							return ErrNoRowsAffected
						}
					}
					if h.CreatedAt.After(maxCreatedAt) {
						maxCreatedAt = h.CreatedAt
					}
				}

				tx = tx.Exec(`
					UPDATE payment_pointer
					SET pointer = ?
					WHERE w_id = ? AND d_id = ?
				`, maxCreatedAt, ptr.Wid, ptr.Did)
				if tx.Error != nil {
					return err
				} else if tx.RowsAffected == 0 {
					return ErrNoRowsAffected
				}

				hasUpdate = deltaDYtd > 0 || deltaWYtd > 0

				return nil
			})
		}
		if err := Retry(compensateTxn); err != nil {
			logs.Printf("compensate txn failed: %v", err)
			continue
		}
		if hasUpdate {
			nextLastUpdated = time.Now().UTC()
		}
	}

	return nextLastUpdated, nil
}
