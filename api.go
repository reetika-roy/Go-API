package main

import (
    "log"
    "net/http"
    "encoding/xml"
    "github.com/pquerna/ffjson/ffjson"
    "io/ioutil"
    "github.com/gorilla/mux"
    _ "github.com/go-sql-driver/mysql"
    "github.com/patrickmn/go-cache"
    "database/sql"
    "sync"
    "strings"
    "time"
    "runtime"
)

type Data struct {
    ID   string
    Path string
}

type Bk struct{
    Name string `json:"a"`
}

type Result struct {
        Value  []float64 `xml:"a>b>c"`
        Routing string `xml:"d"`
    }

type Resp struct {
    ID string `json:"a"`
    Flow int `json:"b"`
    BkName string `json:"c"`
}

type Respslice struct {
    Resps []Resp `json:"a"`
}

var ch *cache.Cache
var db *sql.DB

func computationWork(wg *sync.WaitGroup, data Data, c chan Resp) {
    wg.Done()
    t := Result{}
    b:= Bk{}
    data1, _ := ioutil.ReadFile(data.Path)
            _ = xml.Unmarshal(data1, &t)
            done := make(chan bool)
            go func() {

                  r1, _ := http.Get("https://some-api.ext/"+t.Routing)

                    body, _ := ioutil.ReadAll(r1.Body)
                    _ = ffjson.Unmarshal(body, &b)
                    done <- true
             }()
            var total float64 = 0.00
            for _, amount := range t.Value{
                total += amount
            }
            <-done
            c <- Resp{ID: data.ID, Flow: int(total), BkName: b.Name}
            stmt, _ := db.Prepare("update DB.table set val1=?, val2=? where `id`=?")
            _, _ = stmt.Exec(int(total), b.Name, data.ID)
}

func main() {
    runtime.GC()
    ch = cache.New(5*time.Minute, 30*time.Second)
    db, _ = sql.Open("mysql", "root:password@tcp(hostname)/")
    router := mux.NewRouter().StrictSlash(true)
    router.HandleFunc("/", Index)
    log.Fatal(http.ListenAndServe(":9797", router))
}

func Index(w http.ResponseWriter, r *http.Request) {
    wg := sync.WaitGroup{}
    data := Data{}
    var res Respslice
    var c chan Resp = make(chan Resp)
    var temp Resp
    var keyString string
    keys := r.URL.Query().Get("key")
    if len(keys) != 0 {
        
        tmp:= strings.Split(keys, ",")
        for _,element := range tmp {
            val, found := ch.Get(element)
            if !found {
                keyString+= `'`+element+`'`
                continue
            }
            data.ID = element
            data.Path = val.(string)
            wg.Add(1)
            go computationWork(&wg, data, c)
            temp = <-c
            res.Resps = append(res.Resps, temp)
        }
        
        if len(keyString) != 0 {
            keyString= strings.Replace(keyString, `''`, `','`, -1)
            // keyString= `'`+strings.Replace(keys, `,`,`','`,-1)+`'`
            rows, _ := db.Query("SELECT `id`, `file_name` FROM DB.table WHERE `id` IN ("+ keyString +")")
            defer rows.Close()
            for rows.Next() {
                
                _ = rows.Scan(&data.ID, &data.Path)
                ch.Set(data.ID, data.Path, cache.DefaultExpiration)
                wg.Add(1)
                go computationWork(&wg, data, c)
                temp = <-c
                res.Resps = append(res.Resps, temp)
            }

        }
        wg.Wait()
        b, _ := ffjson.Marshal(res)
        w.Header().Set("Content-Type", "application/json")
        w.Write(b)
        // ffjson.Pool(b)
    }
}
