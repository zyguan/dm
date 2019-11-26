// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package utils

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/siddontang/go-mysql/mysql"

	"github.com/pingcap/dm/pkg/terror"
)

// ParseMetaData parses mydumper's output meta file and returns binlog position and gtid
func ParseMetaData(filename string) (pos *mysql.Position, gtid string, err error) {
	fd, err := os.Open(filename)
	if err != nil {
		return nil, "", terror.ErrParseMydumperMeta.Generate(err)
	}
	defer fd.Close()

	var logName = ""
	br := bufio.NewReader(fd)
	for {
		line, err := br.ReadString('\n')
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, "", terror.ErrParseMydumperMeta.Generate(err)
		}
		line = strings.TrimSpace(line[:len(line)-1])
		if len(line) == 0 {
			continue
		}
		// ref: https://github.com/maxbube/mydumper/blob/master/mydumper.c#L434
		if strings.Contains(line, "SHOW SLAVE STATUS") {
			// now, we only parse log / pos for `SHOW MASTER STATUS`
			break
		}
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}
		fmt.Println(parts[0])
		if parts[0] == "Log" {
			logName = strings.TrimSpace(parts[1])
		} else if parts[0] == "Pos" {
			pos64, err := strconv.ParseUint(strings.TrimSpace(parts[1]), 10, 32)
			if err != nil {
				return nil, "", terror.ErrParseMydumperMeta.Generate(err)
			}
			if len(logName) > 0 {
				pos = &mysql.Position{Name: logName, Pos: uint32(pos64)}
			} else {
				return nil, "", terror.ErrParseMydumperMeta.Generate(fmt.Sprintf("file %s invalid format", filename)) // Pos extracted, but no Log, error occurred
			}
		} else if parts[0] == "GTID" {
			fmt.Println("gtid")
			gtid = strings.TrimSpace(parts[1])
		}
	}

	return pos, gtid, nil
}
