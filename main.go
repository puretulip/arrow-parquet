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
			Type: arrow.BinaryTypes.String, // 초기에는 STRING 타입으로 설정
		},
		{
			Name: "name",
			Type: arrow.BinaryTypes.String, // STRING 타입으로 설정
		},
	}

	// 예제 데이터
	data := []map[string]interface{}{
		{
			"name":    "Bob",
			"hobbies": map[string]string{"activity": "cycling"}, // 리스트가 아닌 단일 값
		},
		{
			"name":    "Alice",
			"hobbies": []map[string]string{{"activity": "reading books"}, {"activity": "playing piano"}},
		},
		{
			"name":    []string{"Charlie", "Charles"}, // name 필드가 리스트인 경우
			"hobbies": []map[string]string{{"activity": "swimming"}, {"activity": "running"}},
		},
	}

	// 스키마를 업데이트해야 하는지 확인
	for i, field := range fields {
		for _, record := range data {
			// 필드가 리스트인지 아닌지 확인
			switch value := record[field.Name].(type) {
			case []map[string]string:
				// 필드가 리스트라면, 스키마를 리스트 타입으로 변경
				var structFields []arrow.Field
				for k := range value[0] {
					structFields = append(structFields, arrow.Field{Name: k, Type: arrow.BinaryTypes.String})
				}
				fields[i] = arrow.Field{
					Name: field.Name,
					Type: arrow.ListOf(arrow.StructOf(structFields...)),
				}
			case []string:
				// 필드가 리스트라면, 스키마를 리스트 타입으로 변경
				fields[i] = arrow.Field{
					Name: field.Name,
					Type: arrow.ListOf(arrow.BinaryTypes.String),
				}
			default:
				// 필드가 리스트가 아니라면 에러 메시지 출력
				log.Printf("Expected %s to be a list but it was not: %v", field.Name, value)
			}
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
		for i, field := range fields {
			switch value := record[field.Name].(type) {
			case string:
				b.Field(i).(*array.StringBuilder).Append(value)
			case map[string]string:
				// 단일 값인 경우 처리
				for _, v := range value {
					b.Field(i).(*array.StringBuilder).Append(v)
				}
			case []map[string]string:
				listBuilder := b.Field(i).(*array.ListBuilder)
				listBuilder.Append(true)
				structBuilder := listBuilder.ValueBuilder().(*array.StructBuilder)
				structType := structBuilder.Type().(*arrow.StructType)
				for _, item := range value {
					structBuilder.Append(true)
					for j := 0; j < structBuilder.NumField(); j++ {
						fieldName := structType.Field(j).Name
						structBuilder.FieldBuilder(j).(*array.StringBuilder).Append(item[fieldName])
					}
				}
			case []string:
				listBuilder := b.Field(i).(*array.ListBuilder)
				listBuilder.Append(true)
				stringBuilder := listBuilder.ValueBuilder().(*array.StringBuilder)
				for _, item := range value {
					stringBuilder.Append(item)
				}
			default:
				// 필드가 리스트가 아니라면 단일 값으로 처리
				b.Field(i).(*array.StringBuilder).Append(fmt.Sprintf("%v", value))
			}
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
