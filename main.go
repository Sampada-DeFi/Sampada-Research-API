package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/csv"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	"github.com/gorilla/mux"
	"google.golang.org/api/iterator"
)

func IndexHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	year := vars["year"]
	quarter := vars["quarter"]
	//bucket name contains info about account will initialize later
	object := "SEC/" + year + "/QTR" + quarter + "/xbrl.gz"
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()
	rc, err := client.Bucket(bucket).Object(object).ReadCompressed(true).NewReader(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer rc.Close()
	gzr, err := gzip.NewReader(rc)
	if err != nil {
		log.Fatal(err)
	}
	defer gzr.Close()

	body, err := ioutil.ReadAll(gzr)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Fprint(w, string(body))
}

type fundamentalDataXBRLRow struct {
	Year              int
	Quarter           int
	CIK               string
	AccessionNum      string
	Axis              string
	Abstract          string
	Tag               string
	Value             string
	UnitOfMeasurement string
	Description       string
}

func FinancialStatementHandler(client *bigquery.Client, w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	year := vars["year"]
	quarter := vars["quarter"]
	statement_type := vars["statementtype"]

	//projectID contains information about google cloud account will initialize later
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		fmt.Fprint(w, err)
	}
	q := client.Query("SELECT * FROM `" + projectID + ".operating_companies." + statement_type + "`WHERE year=" + year + " AND quarter=" + quarter)
	q.Location = "US"
	job, err := q.Run(ctx)
	if err != nil {
		fmt.Fprint(w, err)
	}
	status, err := job.Wait(ctx)
	if err != nil {
		fmt.Fprint(w, err)
	}
	if err := status.Err(); err != nil {
		fmt.Fprint(w, err)
	}
	it, _ := job.Read(ctx)
	var csv_file [][]string
	for {
		var csv_row []string
		var row fundamentalDataXBRLRow
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		csv_row = append(csv_row, strconv.Itoa(row.Year), strconv.Itoa(row.Quarter), row.CIK, row.AccessionNum, row.Axis, row.Abstract, row.Tag, row.Value, row.UnitOfMeasurement, row.Description)
		csv_file = append(csv_file, csv_row)
	}
	client.Close()
	buffer := &bytes.Buffer{}
	writer := csv.NewWriter(buffer)
	writer.WriteAll(csv_file)
	if err := writer.Error(); err != nil {
		log.Fatalln("error writing csv:", err)
	}
	w.Header().Set("Content-Type", "text/csv")
	w.Header().Set("Content-Disposition", "attachment;filename=dei.csv")
	w.Write(buffer.Bytes())
}

type documentEntityInformationXBRLRow struct {
	Year         int
	Quarter      int
	CIK          string
	AccessionNum string
	Tag          string
	Value        string
	Description  string
}

func main() {
	r := mux.NewRouter()
	r.HandleFunc("/xbrl/fundamental/{year}/{quarter}/{statementtype}", FinancialStatementHandler)
	r.HandleFunc("/xbrl/overview/{year}/{quarter}", DocumentEntityInformationHandler)
	r.HandleFunc("/xbrl/index/{year}/{quarter}", IndexHandler)
	http.Handle("/", r)
	log.Fatal(http.ListenAndServe(":8080", nil))
}
