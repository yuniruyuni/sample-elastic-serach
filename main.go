package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	es "github.com/elastic/go-elasticsearch/v7"
	esapi "github.com/elastic/go-elasticsearch/v7/esapi"
	"log"
	"strings"
)

func readInfosFromES(c *es.Client) error {
	res, err := c.Info()
	if err != nil {
		log.Fatalf("Error getting response: %s", err)
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		log.Fatalf("Error: %s", res.String())
		return errors.New(res.String())
	}

	var r map[string]interface{}
	if err = json.NewDecoder(res.Body).Decode(&r); err != nil {
		log.Fatalf("Error parsing the response body: %s", err)
		return err
	}
	log.Printf("Client: %s", es.Version)
	log.Printf("Server: %s", r["version"].(map[string]interface{})["number"])
	return nil
}

func indexDocument(c *es.Client) error {
	// index some documents
	doc := `{"name": "John Doe"}`

	req := esapi.IndexRequest{
		Index:      "test-index",
		DocumentID: "john-due",
		Body:       strings.NewReader(doc),
		Refresh:    "true",
	}

	res, err := req.Do(context.Background(), c)
	if err != nil {
		log.Fatalf("Error getting response: %s", err)
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		log.Printf("Error indexing document: %s", res.Status())
		return errors.New(res.Status())
	}

	// Deserialize the response into a map.
	var r map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		log.Printf("Error parsing the response body: %s", err)
		return err
	}

	// Print the response status and indexed document version.
	log.Printf("[%s] %s; version=%d", res.Status(), r["result"], int(r["_version"].(float64)))
	return nil
}

func search(c *es.Client) error {
	// query search condition
	// it is defined as json and so we need generate query json.
	var buf bytes.Buffer
	var r map[string]interface{}
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"match": map[string]interface{}{
				"name": "John",
			},
		},
	}
	if err := json.NewEncoder(&buf).Encode(query); err != nil {
		log.Fatalf("Error encoding query: %s", err)
		return err
	}

	// Perform the search request.
	res, err := c.Search(
		c.Search.WithContext(context.Background()),
		c.Search.WithIndex("test-index"),
		c.Search.WithBody(&buf),
		c.Search.WithTrackTotalHits(true),
		// I think we don't need this parameter for machine.
		// c.Search.WithPretty(),
	)
	if err != nil {
		log.Fatalf("Error getting response: %s", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		var e map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
			log.Fatalf("Error parsing the response body: %s", err)
			return err
		}

		// Print the response status and error information.
		log.Fatalf("[%s] %s: %s",
			res.Status(),
			e["error"].(map[string]interface{})["type"],
			e["error"].(map[string]interface{})["reason"],
		)
		return errors.New("decoding error")
	}

	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		log.Fatalf("Error parsing the response body: %s", err)
		return err
	}

	// Print the response status, number of results, and request duration.
	log.Printf(
		"[%s] %d hits; took: %dms",
		res.Status(),
		int(r["hits"].(map[string]interface{})["total"].(map[string]interface{})["value"].(float64)),
		int(r["took"].(float64)),
	)

	// Print the ID and document source for each hit.
	for _, hit := range r["hits"].(map[string]interface{})["hits"].([]interface{}) {
		log.Printf(" * ID=%s, %s", hit.(map[string]interface{})["_id"], hit.(map[string]interface{})["_source"])
	}

	return nil
}

func main() {

	cfg := es.Config{
		Addresses: []string{
			"http://localhost:9200",
		},
		// following parameter is useful if you want to use BasicAuth for the cluster access.
		// Username: "foo",
		// Password: "bar",

		// also you can config following Transport parameters
		// Transport: &http.Transport{
		// 	MaxIdleConnsPerHost: 10,
		// 	ResponseHeaderTimeout: time.Second,
		// 	TLSClientConfig: &tls.Config {
		// 		MinVersion: tls.VersionTLS11,
		// 	}
		// },
	}

	c, err := es.NewClient(cfg)
	if err != nil {
		log.Fatalf("Error creating the client: %s", err)
		return
	}
	if err := readInfosFromES(c); err != nil {
		return
	}
	if err := indexDocument(c); err != nil {
		return
	}
	if err := search(c); err != nil {
		return
	}
}
