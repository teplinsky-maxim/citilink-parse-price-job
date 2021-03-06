package main

import (
	"bytes"
	"fmt"
	"github.com/anaskhan96/soup"
	"github.com/google/uuid"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"io/ioutil"
	"log"
	"net/http"
	"path"
	"regexp"
	"strconv"
	"strings"
	"text/template"
	"time"

	"context"
	yc "github.com/ydb-platform/ydb-go-yc"
)

const (
	LinkToParse = "https://www.citilink.ru/catalog/noutbuki/?text=&sorting=price_asc&f=discount.any%2Crating.any%2C277_3cored1i7%2C19967_316d1gb%2C18332_31d1tb%2C9625_3&pf=discount.any%2Crating.any%2C277_3cored1i7%2C19967_316d1gb%2C18332_31d1tb"
)

type Info struct {
	Link  string
	Name  string
	Price uint32
}

type Response struct {
	StatusCode int         `json:"statusCode"`
	Body       interface{} `json:"body"`
}

func main() {
	ctx := context.Background()
	_, err := Handler(ctx)
	if err != nil {
		log.Fatal(err)
	}
}

func Handler(ctx context.Context) (*Response, error) {
	content := getLinkContent()
	data := parseContent(content)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	db := initializeDatabase(ctx)
	defer func() {
		_ = db.Close(ctx)
	}()

	createTables(db, ctx)
	insertData(db, ctx, data)

	return &Response{
		StatusCode: 200,
		Body:       "OK",
	}, nil
}

func parseContent(content string) []Info {
	doc := soup.HTMLParse(content)

	cityElement := doc.Find("button", "class", "MainHeader__open-text")
	city := cityElement.FullText()
	city = strings.Trim(city, " \n")
	fmt.Printf("%v", city)

	products := doc.FindAll("div", "class", "ProductCardVerticalLayout")
	var result []Info
	for _, product := range products {
		var info Info
		name := product.Find("div", "class", "ProductCardVerticalLayout__wrapper-description")
		if name.Error != nil {
			panic(name.Error)
		}

		notebookName := name.FullText()
		info.parseName(notebookName)

		link := product.Find("a")
		if link.Error != nil {
			panic(link.Error)
		}

		href := link.Attrs()["href"]
		info.parseLink(href)

		price := product.Find("div", "class", "ProductCardVerticalLayout__footer")
		if price.Error != nil {
			panic(price.Error)
		}

		price = price.Find("span", "class", "ProductCardVerticalPrice__price-current_current-price")
		if price.Error != nil {
			info.Price = 0
		} else {
			text := price.FullText()
			info.parsePrice(text)
		}

		result = append(result, info)
	}

	return result
}

func (info *Info) parseName(nodeText string) {
	re := regexp.MustCompile(`??????????????.*`)
	result := re.Find([]byte(nodeText))
	info.Name = string(result)
}

func (info *Info) parseLink(href string) {
	info.Link = href
}

func (info *Info) parsePrice(price string) {
	reg, _ := regexp.Compile(`[^a-zA-Z\d]+`)
	processedString := reg.ReplaceAllString(price, "")
	priceInt, _ := strconv.ParseUint(processedString, 10, 32)
	info.Price = uint32(priceInt)
}

func getLinkContent() string {
	var client http.Client
	req, _ := http.NewRequest("GET", LinkToParse, nil)
	req.AddCookie(&http.Cookie{
		Name:   "_space",
		Value:  "chlb_cl:",
		MaxAge: 300,
	})

	req.AddCookie(&http.Cookie{
		Name:   "_dy_df_geo",
		Value:  "Russia..Chelyabinsk",
		MaxAge: 300,
	})

	req.AddCookie(&http.Cookie{
		Name:   "_dy_df_geo",
		Value:  "RU.EU.RU_CHE.RU_CHE_Chelyabinsk",
		MaxAge: 300,
	})

	resp, err := client.Do(req)
	if err != nil {
		log.Fatalln(err)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatalln(err)
	}

	sb := string(body)
	return sb
}

func initializeDatabase(ctx context.Context) ydb.Connection {
	db, err := ydb.Open(
		ctx,
		"grpcs://ydb.serverless.yandexcloud.net:2135/?database=/ru-central1/b1gismdbbpt0a6u6siv9/etn159kbte5q3m3c2qv1",
		//"grpc://localhost:2136/?database=/local",
		yc.WithInternalCA(),
		yc.WithServiceAccountKeyFileCredentials("key.json"),
		//ydb.WithAnonymousCredentials(),
	)
	if err != nil {
		log.Fatal(err)
	}
	return db
}

func createTables(db ydb.Connection, ctx context.Context) {
	err := db.Table().Do(
		ctx,
		func(ctx context.Context, s table.Session) (err error) {
			return s.CreateTable(ctx, path.Join(db.Name(), "price"),
				options.WithColumn("id", types.Optional(types.TypeUTF8)),
				options.WithColumn("link", types.Optional(types.TypeUTF8)),
				options.WithColumn("name", types.Optional(types.TypeUTF8)),
				options.WithColumn("price", types.Optional(types.TypeUint32)),
				options.WithColumn("time_created", types.Optional(types.TypeUint64)),
				options.WithPrimaryKeyColumn("id"),
			)
		},
	)
	if err != nil {
		log.Fatal(err)
	}
}

type templateConfig struct {
	TablePathPrefix string
}

var myWriteQuery = template.Must(template.New("upsert").Parse(`
PRAGMA TablePathPrefix("{{ .TablePathPrefix }}");

DECLARE $values AS List<Struct<
	id: Utf8,
	link: Utf8,
	name: Utf8,
	price: Uint32,
	time_created: Uint64
>>;

REPLACE INTO price
SELECT 
	id, link, name, price, time_created
FROM AS_TABLE($values);
`))

func insertData(db ydb.Connection, ctx context.Context, data []Info) {
	err := db.Table().Do(
		ctx,
		func(ctx context.Context, s table.Session) (err error) {
			txc := table.TxControl(
				table.BeginTx(
					table.WithSerializableReadWrite(),
				),
				table.CommitTx(),
			)

			var values []types.Value
			for _, item := range data {
				uuidToInsert := uuid.New().String()
				values = append(values, types.StructValue(
					types.StructFieldValue("id", types.UTF8Value(uuidToInsert)),
					types.StructFieldValue("link", types.UTF8Value(item.Link)),
					types.StructFieldValue("name", types.UTF8Value(item.Name)),
					types.StructFieldValue("price", types.Uint32Value(item.Price)),
					types.StructFieldValue("time_created", types.Uint64Value(uint64(time.Now().Unix()))),
				))
			}
			list := types.ListValue(values...)
			params := table.NewQueryParameters(
				table.ValueParam("$values", list),
			)

			_, _, err = s.Execute(
				ctx,
				txc,
				render(myWriteQuery, templateConfig{
					TablePathPrefix: db.Name(),
				}),
				params,
			)
			return err
		},
	)
	if err != nil {
		panic(err)
	}
}

func render(t *template.Template, data interface{}) string {
	var buf bytes.Buffer
	err := t.Execute(&buf, data)
	if err != nil {
		panic(err)
	}
	return buf.String()
}
