package main

import "time"

type CustomerInfo struct {
	CWId       int64
	CDId       int64
	CId        int64
	CFirst     string
	CMiddle    string
	CLast      string
	CStreet1   string
	CStreet2   string
	CCity      string
	CState     string
	CZip       string
	CPhone     string
	CSince     time.Time
	CCredit    string
	CCreditLim float64
	CDiscount  float64
	CData      string
}

// type CustomerInfo struct {
// 	CWId       int64     `gorm:"column:c_w_id"`
// 	CDId       int64     `gorm:"column:c_d_id"`
// 	CId        int64     `gorm:"column:c_id"`
// 	CFirst     string    `gorm:"column:c_first"`
// 	CMiddle    string    `gorm:"column:c_middle"`
// 	CLast      string    `gorm:"column:c_last"`
// 	CStreet1   string    `gorm:"column:c_street_1"`
// 	CStreet2   string    `gorm:"column:c_street_2"`
// 	CCity      string    `gorm:"column:c_city"`
// 	CState     string    `gorm:"column:c_state"`
// 	CZip       string    `gorm:"column:c_zip"`
// 	CPhone     string    `gorm:"column:c_phone"`
// 	CSince     time.Time `gorm:"column:c_since"`
// 	CCredit    string    `gorm:"column:c_credit"`
// 	CCreditLim float64   `gorm:"column:c_credit_lim"`
// 	CDiscount  float64   `gorm:"column:c_discount"`
// 	CData      string    `gorm:"column:c_data"`
// }

type DistrictInfo struct {
	DId      int64   `gorm:"column:d_id"`
	DWId     int64   `gorm:"column:d_w_id"`
	WName    string  `gorm:"column:w_name"`
	WStreet1 string  `gorm:"column:w_street_1"`
	WStreet2 string  `gorm:"column:w_street_2"`
	WCity    string  `gorm:"column:w_city"`
	WState   string  `gorm:"column:w_state"`
	WZip     string  `gorm:"column:w_zip"`
	WTax     float64 `gorm:"column:w_tax"`
	DName    string  `gorm:"column:d_name"`
	DStreet1 string  `gorm:"column:d_street_1"`
	DStreet2 string  `gorm:"column:d_street_2"`
	DCity    string  `gorm:"column:d_city"`
	DState   string  `gorm:"column:d_state"`
	DZip     string  `gorm:"column:d_zip"`
	DTax     float64 `gorm:"column:d_tax"`
}
