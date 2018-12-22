package main

import (
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/go-sql-driver/mysql"

	yaml "gopkg.in/yaml.v2"

	"github.com/labstack/echo"
	"github.com/labstack/echo/middleware"
)

var db *sql.DB

const (
	dbSourceFilePath = "/etc/credentials/db-connection.yml"
	pemFilePath      = "/etc/credentials/rds-ca-root.pem"
)

type (
	DBSource struct {
		Host     string
		Port     string
		User     string
		Password string
		Database string
	}

	Personality struct {
		ID        int64
		Name      string
		Email     string
		CreatedAt time.Time
		UpdatedAt time.Time
	}
)

func main() {
	e := echo.New()

	e.Use(middleware.Logger())
	e.Use(middleware.Recover())

	if err := initiDB(); err != nil {
		panic(err.Error())
	}
	// defer db.Close()

	e.GET("/", func(c echo.Context) error {
		return c.String(http.StatusOK, "Hello k8s!")
	})

	e.GET("/personalities/:id", func(c echo.Context) error {
		id, err := strconv.ParseInt(c.Param("id"), 10, 64)
		if err != nil {
			return err
		}
		result, err := getPersonalityByID(id)
		if err != nil {
			return err
		}

		return c.JSON(http.StatusOK, result)
	})

	e.GET("/personalities", func(c echo.Context) error {
		result, err := getAllPersonalities()
		if err != nil {
			return err
		}

		return c.JSON(http.StatusOK, result)
	})

	e.Logger.Fatal(e.Start(":1323"))
}

func getPersonalityByID(id int64) ([]Personality, error) {
	sql := "SELECT id, name, mail, created, modified FROM t_speaker WHERE id = %d"

	return runPersonalityQuery(fmt.Sprintf(sql, id), "getPersonalityByID")
}

func getAllPersonalities() ([]Personality, error) {
	sql := "SELECT id, name, mail, created, modified FROM t_speaker"

	return runPersonalityQuery(sql, "getAllPersonalities")
}

func runPersonalityQuery(sql, name string) ([]Personality, error) {
	rows, err := runQuery(sql, name)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var personalities []Personality
	for rows.Next() {
		p := Personality{}
		err := rows.Scan(
			&p.ID,
			&p.Name,
			&p.Email,
			&p.CreatedAt,
			&p.UpdatedAt,
		)
		if err != nil {
			return nil, err
		}
		personalities = append(personalities, p)
	}
	return personalities, nil
}

func runQuery(sql, name string) (*sql.Rows, error) {
	defer timeTrack(time.Now(), name)

	return db.Query(sql)
}

func initiDB() error {
	registerTLSConfig()

	f, err := ioutil.ReadFile(dbSourceFilePath)
	if err != nil {
		return err
	}

	src := &DBSource{}
	if err := yaml.Unmarshal([]byte(f), &src); err != nil {
		return err
	}

	dsn, err := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?tls=custom&parseTime=true",
		src.User,
		src.Password,
		src.Host,
		src.Port,
		src.Database,
	), nil
	if err != nil {
		return err
	}

	db, err = sql.Open("mysql", dsn)
	if err != nil {
		return err
	}

	maxOpenConns := 50
	db.SetMaxOpenConns(maxOpenConns)
	db.SetMaxIdleConns(maxOpenConns * 2)
	db.SetConnMaxLifetime(time.Duration(maxOpenConns) * time.Second)

	return nil
}

func registerTLSConfig() error {
	rootCertPool := x509.NewCertPool()
	pem, err := ioutil.ReadFile(pemFilePath)
	if err != nil {
		return err
	}

	if ok := rootCertPool.AppendCertsFromPEM(pem); !ok {
		return err
	}

	err = mysql.RegisterTLSConfig("custom", &tls.Config{
		RootCAs: rootCertPool,
	})
	return err
}

func timeTrack(start time.Time, name string) {
	elapsed := time.Since(start)
	log.Printf("%s took %s", name, elapsed)
}
