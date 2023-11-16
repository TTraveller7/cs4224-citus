package main

import (
	"bufio"
	"fmt"
	"log"
	"sort"
	"strings"

	"gorm.io/gorm"
)

type TopCustomerInfo struct {
	Wid      int64
	Did      int64
	Cid      int64
	CBalance float64
	CFirst   string
	CMiddle  string
	CLast    string
}

type TopDistrictInfo struct {
	Did   int64
	Wid   int64
	WName string
	DName string
}

func TopBalance(logs *log.Logger, db *gorm.DB, words []string, scanner *bufio.Scanner, lineCount *int) error {
	districts := make([]*TopDistrictInfo, 0)
	db = db.Raw(`
		select d_id, d_w_id, w_name, d_name 
		from district_info 
		LIMIT 10000
	`)
	rows, err := db.Rows()
	if err != nil {
		logs.Printf("top balance get district failed: %v", err)
		return err
	}
	for rows.Next() {
		district := &TopDistrictInfo{}
		if err := rows.Scan(&district.Did, &district.Wid, &district.WName, &district.DName); err != nil {
			logs.Printf("top balance scan district failed: %v", err)
		}
		districts = append(districts, district)
	}

	customerInfos := make([]*TopCustomerInfo, 0)
	getTopBalanceCustomerTxn := func() error {
		return db.Transaction(func(tx *gorm.DB) error {
			for _, d := range districts {
				tx = tx.Raw(`
					select c_w_id, c_d_id, c_id, c_balance 
					from customer_param
					where c_w_id = ? 
					order by c_balance 
					limit 10
				`, d.Wid)
				rows, err := tx.Rows()
				if err != nil {
					return err
				}
				for rows.Next() {
					c := &TopCustomerInfo{}
					if err := rows.Scan(&c.Wid, &c.Did, &c.Cid, &c.CBalance); err != nil {
						return err
					}
					customerInfos = append(customerInfos, c)
				}
			}
			return nil
		})
	}
	if err := Retry(getTopBalanceCustomerTxn); err != nil {
		logs.Printf("top balance get customer failed: %v", err)
		return err
	}

	sort.Slice(customerInfos, func(i, j int) bool {
		return customerInfos[i].CBalance < customerInfos[j].CBalance
	})

	topTenCustomers := customerInfos[:10]

	cids := make([]int64, 0)
	for _, c := range topTenCustomers {
		cids = append(cids, c.Cid)
	}
	db = db.Raw(`
		SELECT c_id, c_first, c_middle, c_last 
		FROM customer_info
		WHERE c_id IN ?
		LIMIT 1
	`, FormatInt64Set(cids))
	rows, err = db.Rows()
	if err != nil {
		logs.Printf("top balance get customer name failed: %v", err)
		return err
	}
	for rows.Next() {
		var cid int64
		var cFirst, cMiddle, cLast string
		if err := rows.Scan(&cid, &cFirst, &cMiddle, &cLast); err != nil {
			logs.Printf("top balance scan customer name failed: %v", err)
			return err
		}
		for _, cinfo := range topTenCustomers {
			if cinfo.Cid == cid {
				cinfo.CFirst = cFirst
				cinfo.CMiddle = cMiddle
				cinfo.CLast = cLast
				break
			}
		}
	}

	sb := strings.Builder{}
	for _, cinfo := range topTenCustomers {
		var dName, wName string
		for _, d := range districts {
			if d.Did == cinfo.Did && d.Wid == cinfo.Wid {
				dName = d.DName
				wName = d.WName
				break
			}
		}

		sb.WriteString(fmt.Sprintf("%s, %s, %s, %s, %s, %v", cinfo.CFirst, cinfo.CMiddle, cinfo.CLast, wName, dName, cinfo.CBalance))
	}
	logs.Printf(sb.String())

	return nil
}
