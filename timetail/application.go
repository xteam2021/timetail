package timetail

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"time"
)

type Application struct {
	file           *os.File
	fileInfo       os.FileInfo
	path           string
	seconds        int
	knownPositions []int64
	debugMode      bool
	canPrint       bool
}

func NewApplication(path string, seconds int, debugMode bool) (result *Application) {
	result = &Application{
		path:      path,
		seconds:   seconds,
		debugMode: debugMode,
		canPrint:  false,
	}
	err := result.OpenFile()
	if err != nil {
		log.Fatalln(err)
	}
	return result
}

var EKnownPosition = errors.New("already known position")

type Record struct {
	//RemoteAddr string `json:"remote_addr"`
	//RequstTime string `json:"request_time"`
	//Host string `json:"host"`
	DateTime string `json:"time_local"`
	//Request string `json:"request"`
	//Status string `json:"status"`
	//BodyBytesSent string `json:"body_bytes_sent"`
	//HttpReferer string `json:"http_referer"`
	//HttpUserAgent string `json:"http_user_agent"`
	//HttpXForwardedFor string `json:"http_x_forwarded_for"`
	//UpstreamName string `json:"upstream_name"`
	//UpsrteamAddr string `json:"upstream_addr"`
	//UpstreamStatus string `json:"upstream_status"`
	//RequestID string `json:""`
}

func (app *Application) debug(v ...interface{}) {
	if app.debugMode {
		os.Stderr.WriteString(fmt.Sprintln(v))
	}
}

func (app *Application) Run() (err error) {
	now := time.Now()
	startPos := int64(0)
	endPos := app.fileInfo.Size()
	app.debug("Run: log size", endPos)
	for {
		halfPos := startPos + (endPos-startPos)/2
		app.debug("Run: startPos", startPos, "endPos", endPos, "halfPos", halfPos)
		app.debug("Run: seeking to halfPos")
		app.file.Seek(halfPos, io.SeekStart)

		var dt time.Time
		var dateParseErr error
		for {

			if app.isKnownPosition(app.GetCurrentPosition()) {
				app.canPrint = true
				break
			}

			lineBuf, readErr := app.ReadLineBuf()
			if readErr == EKnownPosition {
				app.debug("Run: already known position", app.GetCurrentPosition())
				app.canPrint = true
				break
			}

			if lineBuf == nil {
				app.debug("empty bytes from lineBuf")
				continue
			}

			if readErr == io.EOF {
				app.debug("Run: EOF if reached")
				app.canPrint = true
				break
			}
			if len(lineBuf.Bytes()) == 0 {
				app.debug("Run: length on line is zero at position", app.GetCurrentPosition())
				break
			}
			record := Record{}
			jsonParseErr := json.Unmarshal(lineBuf.Bytes(), &record)
			if jsonParseErr != nil {
				app.debug(fmt.Sprintf("error parse json (%s), line:\n%s", jsonParseErr, string(lineBuf.Bytes())))
				continue
			}
			dt, dateParseErr = time.Parse("02/Jan/2006:15:04:05 -0700", record.DateTime)
			if dateParseErr != nil {
				app.debug(fmt.Sprintf("error parse datetime (%s), datetime: %s, line:\n%s",
					dateParseErr,
					record.DateTime,
					string(lineBuf.Bytes()),
				))
				continue
			}

			break
		}

		if app.canPrint {
			break
		}

		dur := time.Second * time.Duration(app.seconds)
		if dt.UTC().Unix() > now.UTC().Add(-dur).Unix() {
			app.debug("Run: record date too big, move endPos to halfPos, from", endPos, "to", halfPos)
			endPos = halfPos
		} else {
			app.debug("Run: record date too small, move startPos to halfPos, from", startPos, "to", halfPos)
			startPos = halfPos
		}
	}
	app.debug("Run: printing result")
	app.PrintToTheEnd()
	return err
}

func (app *Application) ReadLineBuf() (buf *bytes.Buffer, err error) {
	app.SeekToPrevEOL()
	if app.isKnownPosition(app.GetCurrentPosition()) {
		return nil, EKnownPosition
	}
	app.debug("ReadLine: read one line from position", app.GetCurrentPosition())
	posBefore := app.GetCurrentPosition()
	r := bufio.NewReader(app.file)
	var line []byte
	var isPrefix bool
	buf = bytes.NewBuffer([]byte{})
	for {
		line, isPrefix, err = r.ReadLine()
		if err == io.EOF {
			return nil, err
		}
		if err != nil {
			log.Fatalln(err)
		}
		buf.Write(line)
		if isPrefix {
			continue
		} else {
			break
		}
	}
	app.addKnownPosition(posBefore)
	return buf, nil
}

func (app *Application) PrintToTheEnd() {
	r := bufio.NewReader(app.file)
	app.debug("PrintToTheEnd: print from position", app.GetCurrentPosition(), "to the end of file")
	var line []byte
	var isPrefix bool
	var err error
	var buf *bytes.Buffer
	buf = bytes.NewBuffer([]byte{})
	for {
		line, isPrefix, err = r.ReadLine()
		if err == io.EOF {
			return
		}
		if err != nil {
			log.Fatalln(err)
		}
		buf.Write(line)
		if isPrefix {
			continue
		} else {
			fmt.Println(string(buf.Bytes()))
			buf = bytes.NewBuffer([]byte{})
		}
	}
}

func (app *Application) GetCurrentPosition() (result int64) {
	result, err := app.file.Seek(0, io.SeekCurrent)
	if err != nil {
		log.Fatalln(err)
	}
	return result
}

func (app *Application) OpenFile() error {
	fd, err := os.OpenFile(app.path, os.O_RDONLY, 0755)
	if err != nil {
		return err
	}
	app.file = fd
	app.fileInfo, _ = fd.Stat()
	return nil
}

func (app *Application) SeekToPrevEOL() {
	app.debug("SeekToPrevEOL: rewind to prev eol from", app.GetCurrentPosition())
	cursor := app.GetCurrentPosition()
	for {
		b := make([]byte, 1)
		_, err := app.file.Read(b)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalln(err)
		}
		if b[0] == 10 || b[0] == 13 {
			break
		}
		cursor -= 1
		app.file.Seek(cursor, io.SeekStart)
	}
	app.debug("SeekToPrevEOL: position of file from seek prev eol is", app.GetCurrentPosition())
}

func (app *Application) isKnownPosition(pos int64) (result bool) {
	result = false
	for _, knownPosition := range app.knownPositions {
		if knownPosition == pos {
			result = true
		}
	}
	app.debug("isKnownPosition: knownPositions =", app.knownPositions, ", check for position", pos, "is", result)
	return result
}

func (app *Application) addKnownPosition(pos int64) {
	app.debug("addKnownPosition: ", pos)
	app.knownPositions = append(app.knownPositions, pos)
}
