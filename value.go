package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"
)

type ValueType string

const (
	ARRAY ValueType = "*"
	BULK ValueType = "$"
	STRING ValueType = "+"
	INTEGER ValueType = ":"
	ERROR ValueType = "-"
	NULL ValueType = ""
)

type Value struct {
	typ ValueType
	bulk string
	str string
	num int
	err string
	array []Value
}

func readLine(r *bufio.Reader) (string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimSuffix(line, "\r\n"), nil
}

func (v *Value) readArray(reader io.Reader) error {
	r := bufio.NewReader(reader)

	line, err := readLine(r)
	if err != nil {
		return err
	}

	if line[0] != '*' {
		return errors.New("expected array")
	}
	
	arrlen, err := strconv.Atoi(string(line[1:]))
	if err != nil {
		fmt.Println(err)
		return err
	}

	for range arrlen {
		bulk := v.readBulk(r)
		v.array = append(v.array, bulk)
	}
	return nil
}

func (v *Value) readBulk(r *bufio.Reader) Value {
	line, err := readLine(r)
	if err != nil {
		log.Println("error readBulk(): ", err)
		return Value{}
	}
	n, err := strconv.Atoi(line[1:])
	if err != nil {
		fmt.Println(err)
		return Value{}
	}

	buf := make([]byte, n+2)
	if _, err := io.ReadFull(r, buf); err != nil {
		fmt.Println(err)
		return Value{}
	}
	
	bulk := string(buf[:n])
	return Value{typ: BULK, bulk: bulk}
}
