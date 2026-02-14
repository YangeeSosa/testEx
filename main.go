package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"log"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
	"github.com/xuri/excelize/v2"
)

func uploadHandler(w http.ResponseWriter, r *http.Request, db *sql.DB) {
	if r.Method != http.MethodPost {
		http.Error(w, "нужен метод POST", http.StatusMethodNotAllowed)
		return
	}

	err := r.ParseMultipartForm(32 << 20)
	if err != nil {
		http.Error(w, fmt.Sprintf("ошибка парсинга формы: %s", err), http.StatusBadRequest)
		return
	}

	file, header, err := r.FormFile("file")
	if err != nil {
		http.Error(w, fmt.Sprintf("не удаётся получить поля: %v", err), http.StatusBadRequest)
		return
	}
	defer file.Close()

	fileName := header.Filename
	fmt.Fprintf(w, "Получен файл: %s, размер файла: %d байт\n", fileName, header.Size)

	tableName := "uploaded_data"

	ext := filepath.Ext(fileName)

	var headers []string
	var rows [][]string

	switch ext {
	case ".csv":
		reader := csv.NewReader(file)
		reader.Comma = ';'

		records, err := reader.ReadAll()
		if err != nil {
			http.Error(w, fmt.Sprintf("Ошибка чтения файла: %v", err), http.StatusBadRequest)
			return
		}
		if len(records) < 2 {
			http.Error(w, "В таблице должен быть заголовок и хотя бы 1 запись", http.StatusBadRequest)
			return
		}
		headers = records[0]
		rows = records[1:]

	case ".xlsx":
		h, r, err := readXLSX(file)
		if err != nil {
			http.Error(w, fmt.Sprintf("Ошибка чтения файла: %v", err), http.StatusBadRequest)
			return
		}
		headers = h
		rows = r

	default:
		http.Error(w, "данный формат не поддерживается", http.StatusBadRequest)
		return
	}

	colTypes := indentColumnTypes(headers, rows)

	if err := createTable(db, tableName, headers, colTypes); err != nil {
		http.Error(w, fmt.Sprintf("ошибка создания таблицы: %v", err), http.StatusInternalServerError)
		return
	}

	if err := insertRows(db, tableName, headers, rows); err != nil {
		http.Error(w, fmt.Sprintf("ошибка вставки строк: %v", err), http.StatusInternalServerError)
		return
	}

	fmt.Fprintf(w, "Колонки: %v\n", headers)
	fmt.Fprintf(w, "Кол-во строк: %d\n", len(rows))
}

func readXLSX(file multipart.File) ([]string, [][]string, error) {
	f, err := excelize.OpenReader(file)
	if err != nil {
		return nil, nil, fmt.Errorf("ошибка открытия файла: %v", err)
	}
	defer f.Close()

	sheetName := f.GetSheetName(0)
	if sheetName == "" {
		return nil, nil, fmt.Errorf("в файле нет листов")
	}

	rowsIter, err := f.Rows(sheetName)
	if err != nil {
		return nil, nil, fmt.Errorf("ошибка чтения строк: %v", err)
	}

	var headers []string
	var rows [][]string
	rowIndex := 0
	for rowsIter.Next() {
		row, err := rowsIter.Columns()
		if err != nil {
			return nil, nil, fmt.Errorf("ошибка чтения строк: %v", err)
		}
		if rowIndex == 0 {
			headers = row
		} else {
			rows = append(rows, row)
		}
		rowIndex++
	}

	if len(headers) == 0 {
		return nil, nil, fmt.Errorf("нет заголовков")
	}

	return headers, rows, nil
}

func indentColumnTypes(headers []string, rows [][]string) []string {
	types := make([]string, len(headers))

	for col := range headers {
		isNum := true
		for _, row := range rows {
			if col >= len(row) {
				continue
			}
			val := strings.TrimSpace(row[col])
			if val == "" {
				continue
			}
			if _, err := strconv.ParseFloat(val, 64); err != nil {
				isNum = false
				break
			}
		}
		if isNum {
			types[col] = "NUMERIC"
		} else {
			types[col] = "VARCHAR"
		}
	}
	return types
}

func quoteIdent(ident string) string {
	ident = strings.ReplaceAll(ident, `"`, `""`)
	return `"` + ident + `"`
}

func createTable(db *sql.DB, tableName string, headers []string, columnTypes []string) error {
	if len(headers) == 0 {
		return fmt.Errorf("нет колонок")
	}
	cols := make([]string, len(headers))
	for i, h := range headers {
		colName := h
		cType := columnTypes[i]
		if cType == "" {
			cType = "VARCHAR"
		}
		cols[i] = fmt.Sprintf("%s %s", quoteIdent(colName), cType)
	}

	sqlStr := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", quoteIdent(tableName), strings.Join(cols, ", "))

	_, err := db.Exec(sqlStr)
	if err != nil {
		return fmt.Errorf("ошибка создания таблицы: %v", err)
	}
	return nil
}

func insertRows(db *sql.DB, tableName string, headers []string, rows [][]string) error {
	if len(rows) == 0 {
		return nil
	}

	colNames := make([]string, len(headers))
	for i, h := range headers {
		colNames[i] = quoteIdent(h)
	}

	placeHolders := make([]string, len(headers))
	for i := range placeHolders {
		placeHolders[i] = fmt.Sprintf("$%d", i+1)
	}

	stmtTxt := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", quoteIdent(tableName), strings.Join(colNames, ", "), strings.Join(placeHolders, ", "))

	for _, row := range rows {
		args := make([]any, len(headers))
		for i := range headers {
			if i < len(row) {
				args[i] = row[i]
			} else {
				args = nil
			}
		}
		if _, err := db.Exec(stmtTxt, args...); err != nil {
			return fmt.Errorf("ршибка вставки: %w", err)
		}
	}
	return nil
}

func main() {
	_ = godotenv.Load()

	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		log.Fatal("не задан DATABASE_URL")
	}

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		log.Fatalf("ошибка открытия БД: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatalf("не проходят пинги: %v", err)
	}

	http.HandleFunc("/upload", func(w http.ResponseWriter, r *http.Request) {
		uploadHandler(w, r, db)
	})

	addr := ":8080"
	log.Printf("Сервер слушает по адресу: %s", addr)
	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Fatal(err)
	}
}

