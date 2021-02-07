package main

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/mattn/go-sqlite3"
	"sync"
	"time"
)

const (
	ADR    = "127.0.0.1:3306"
	USER   = "root"
	PWD    = "220884"
	DBNAME = "golang"
)

type chot struct {
	c  int
	mu sync.Mutex
}

var ch = chot{
	c:  0,
	mu: sync.Mutex{},
}

var wg sync.WaitGroup

func addDB(i int, s *sql.DB) {

	defer wg.Done()

	// отправляем запрос
	_, err := s.Exec("INSERT INTO MyGuests (firstname,lastname) VALUES(?,?)", "kldskldks", "739173298")

	// проверяем ошибки
	if err != nil {

		// [mysql]если сервер ответил что сликом много подключений
		if err.Error() == "Error 1040: Too many connections" {
			fmt.Println(err)
			time.Sleep(1 * time.Second)
			wg.Add(1)
			addDB(i, s)
			return
		}

		// [mysql]
		if err.Error() == fmt.Sprintf("dial tcp %s: connectex: No connection could be made because the target machine actively refused it.", ADR) {
			fmt.Println(err)
			time.Sleep(1 * time.Second)
			wg.Add(1)
			addDB(i, s)
			return
		}

		// [sqLite]
		if err.Error() == "database is locked" {
			fmt.Println(err)
			time.Sleep(1 * time.Second)
			wg.Add(1)
			go addDB(i, s)
			return
		}

		// какаято другая ошибка
		fmt.Printf("%d. addDB: %s\n", i, err.Error())
		return
	}

	// завершаем запрос
	//r.Close()
	// увеличиваем счетчик
	ch.mu.Lock()
	ch.c++

	fmt.Printf("%d. addDB: %s %d\n", i, "успешно", ch.c)
	ch.mu.Unlock()

}

func mySql() (db *sql.DB, er error) {

	er = nil

	//строка подключения
	d := fmt.Sprintf("%s:%s@tcp(%s)/%s", USER, PWD, ADR, DBNAME)
	// отекрываем соединение
	db, err := sql.Open("mysql", d)
	if err != nil {
		er = fmt.Errorf("sql.Open: %s", err.Error())
		return
	}

	// максимально доличество соединений в пуле
	db.SetMaxOpenConns(100)

	// время
	db.SetConnMaxIdleTime(10 * time.Second)

	// содаем таблицу
	_, err = db.Exec(`CREATE TABLE MyGuests (
								id INT(6) UNSIGNED AUTO_INCREMENT PRIMARY KEY,
								firstname VARCHAR(30) NOT NULL,
								lastname VARCHAR(30) NOT NULL,
								email JSON NOT NULL,
								reg_date TIMESTAMP 
								)`)

	if err != nil && err.Error() != "Error 1050: Table 'MyGuests' already exists" {
		er = fmt.Errorf("db.Exec: %v", err)
		return
	}

	return
}

func sqLite() (db *sql.DB, er error) {

	er = nil

	// отекрываем соединение
	db, err := sql.Open("sqlite3", "sql.db")
	if err != nil {
		er = fmt.Errorf("sql.Open: %s", err.Error())
		return
	}

	// максимально доличество соединений в пуле
	//db.SetMaxOpenConns(1)

	// время
	//db.SetConnMaxIdleTime(10 * time.Second)

	// содаем таблицу
	_, err = db.Exec(`CREATE TABLE MyGuests (
								id INTEGER primary key autoincrement,
								firstname VARCHAR,
								lastname VARCHAR,
								email TEXT,
								reg_date TEXT
								)`)

	if err != nil && err.Error() != "table MyGuests already exists" {

		er = fmt.Errorf("db.Exec: %v", err)
		return
	}

	return

}

func main() {

	db, err := sqLite()
	//db, err := mySql()
	if err != nil {
		fmt.Println(err)
		return
	}

	// засекаем время
	start := time.Now()


	// создаем запросы
	for i := 0; i < 3000; i++ {
		wg.Add(1)
		// запускаем горутину
		go addDB(i, db)
	}

	// ожидаем завершения горутин
	wg.Wait()

	// виксируем время выполнения
	duration := time.Since(start)
	fmt.Printf("END %v\n", duration)

	// статистика по соединениям
	dc := db.Stats()
	fmt.Printf("%#v\n", dc)


	// проверка соединения с БД
	if err := db.Ping(); err != nil {
		fmt.Println(err)
	}

	// закрываем соединение
	db.Close()

}
