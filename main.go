package main

import (
	"fmt"
	"log"
	"os"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/apache/arrow/go/v13/parquet"
	"github.com/apache/arrow/go/v13/parquet/compress"
	"github.com/apache/arrow/go/v13/parquet/pqarrow"
)

func main() {
	// Arrow 필드 정의
	fields := []arrow.Field{
		{
			Name: "hobbies",
			Type: arrow.ListOf(
				arrow.StructOf(
					arrow.Field{Name: "activity", Type: arrow.BinaryTypes.String}, // STRING 타입으로 설정
				),
			),
		},
		{
			Name: "name",
			Type: arrow.BinaryTypes.String, // STRING 타입으로 설정
		},
	}

	// 예제 데이터
	data := []map[string]interface{}{
		{
			"name":    "Alice",
			"hobbies": []map[string]string{{"activity": "reading books"}, {"activity": "playing piano"}},
		},
		{
			"name":    "Bob",
			"hobbies": map[string]string{"activity": "cycling"}, // 리스트가 아닌 단일 값
		},
	}

	// 스키마를 업데이트해야 하는지 확인
	isListType := false
	for _, record := range data {
		// hobbies가 리스트인지 아닌지 확인
		switch hobbies := record["hobbies"].(type) {
		case []map[string]string:
			// hobbies 필드가 리스트라면, 스키마를 리스트 타입으로 변경
			isListType = true
		default:
			// hobbies 필드가 리스트가 아니라면 에러 메시지 출력
			log.Printf("Expected hobbies to be a list but it was not: %v", hobbies)
		}
	}

	if isListType {
		fields[0] = arrow.Field{
			Name: "hobbies",
			Type: arrow.ListOf(
				arrow.StructOf(
					arrow.Field{Name: "activity", Type: arrow.BinaryTypes.String}, // STRING 타입으로 설정
				),
			),
		}
	}

	// Arrow 스키마 생성
	schema := arrow.NewSchema(fields, nil)

	// Arrow 메모리 풀 생성
	pool := memory.NewGoAllocator()

	// Arrow 레코드 배열 생성
	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()

	for _, record := range data {
		b.Field(1).(*array.StringBuilder).Append(record["name"].(string))

		hobbiesBuilder := b.Field(0).(*array.ListBuilder)
		hobbiesBuilder.Append(true)
		hobbyStructBuilder := hobbiesBuilder.ValueBuilder().(*array.StructBuilder)
		activityBuilder := hobbyStructBuilder.FieldBuilder(0).(*array.StringBuilder)

		switch hobbies := record["hobbies"].(type) {
		case []map[string]string:
			for _, hobby := range hobbies {
				hobbyStructBuilder.Append(true)
				activityBuilder.Append(hobby["activity"])
			}
		case map[string]string:
			hobbyStructBuilder.Append(true)
			activityBuilder.Append(hobbies["activity"])
		}
	}

	recordArray := b.NewRecord()
	defer recordArray.Release()

	// Parquet 파일 생성
	outFile, err := os.Create("output.parquet")
	if err != nil {
		log.Fatal("failed to create output writer")
	}
	defer outFile.Close()

	// WriterProperties 및 ArrowWriterProperties 생성
	props := parquet.NewWriterProperties(
		parquet.WithCompression(compress.Codecs.Snappy),
		parquet.WithRootName("my_schema"),
		parquet.WithRootRepetition(parquet.Repetitions.Required),
	)

	// Parquet 파일에 Arrow 데이터를 쓰기 위한 Writer 생성
	pqWriter, err := pqarrow.NewFileWriter(schema, outFile, props, pqarrow.DefaultWriterProps())
	if err != nil {
		log.Fatal("failed to create parquet writer")
	}
	defer pqWriter.Close()

	// 데이터를 Parquet 파일에 기록
	err = pqWriter.WriteBuffered(recordArray)
	if err != nil {
		log.Fatalf("failed to write to parquet file: %v", err)
	}

	fmt.Println("Parquet 파일 생성 완료: output.parquet")
}
