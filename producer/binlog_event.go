package producer

import (
	"github.com/satori/go.uuid"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
)

type BinlogEvent struct {
	Header *BinlogEventHeader
	Event  interface{}
}

func NewBinlogEvent(binlogEv *replication.BinlogEvent) *BinlogEvent {
	newEv := &BinlogEvent{}
	newEv.Header = NewBinlogEventHeader(binlogEv.Header)
	switch ev := binlogEv.Event.(type) {
	case *replication.FormatDescriptionEvent:
		newEv.Event = WrapFormatDescriptionEvent(ev)
	case *replication.RotateEvent:
		newEv.Event = WrapRotateEvent(ev)
	case *replication.QueryEvent:
		newEv.Event = WrapQueryEvent(ev)
	case *replication.GTIDEvent:
		newEv.Event = WrapGTIDEvent(ev)
	case *replication.BeginLoadQueryEvent:
		newEv.Event = WrapBeginLoadQueryEvent(ev)
	case *replication.MariadbAnnotaeRowsEvent:
		newEv.Event = WrapMariadbAnnotaeRowsEvent(ev)
	case *replication.MariadbBinlogCheckPointEvent:
		newEv.Event = WrapMariadbBinlogCheckPointEvent(ev)
	case *replication.TableMapEvent:
		newEv.Event = WrapTableMapEvent(ev)
	case *replication.RowsEvent:
		newEv.Event = WrapRowsEvent(ev)
	case *replication.RowsQueryEvent:
		newEv.Event = WrapRowsQueryEvent(ev)
	default:
		newEv.Event = ev
	}
	return newEv
}

type BinlogEventHeader struct {
	Timestamp uint32
	EventType string
	ServerID  uint32
	EventSize uint32
	LogPos    uint32
	Flags     uint16
}

func NewBinlogEventHeader(header *replication.EventHeader) *BinlogEventHeader {
	return &BinlogEventHeader{
		header.Timestamp,
		header.EventType.String(),
		header.ServerID,
		header.EventSize,
		header.LogPos,
		header.Flags,
	}
}

type FormatDescriptionEvent struct {
	Version                uint16
	ServerVersion          string
	CreateTimestamp        uint32
	EventHeaderLength      uint8
	EventTypeHeaderLengths string
	ChecksumAlgorithm      byte
}

func WrapFormatDescriptionEvent(
	ev *replication.FormatDescriptionEvent,
) *FormatDescriptionEvent {
	return &FormatDescriptionEvent{
		Version:                ev.Version,
		ServerVersion:          string(ev.ServerVersion),
		CreateTimestamp:        ev.CreateTimestamp,
		EventHeaderLength:      ev.EventHeaderLength,
		EventTypeHeaderLengths: string(ev.EventTypeHeaderLengths),
		ChecksumAlgorithm:      ev.ChecksumAlgorithm,
	}
}

type RotateEvent struct {
	Position    uint64
	NextLogName string
}

func WrapRotateEvent(ev *replication.RotateEvent) *RotateEvent {
	return &RotateEvent{
		Position:    ev.Position,
		NextLogName: string(ev.NextLogName),
	}
}

type QueryEvent struct {
	SlaveProxyID  uint32
	ExecutionTime uint32
	ErrorCode     uint16
	StatusVars    []byte
	Schema        string
	Query         string
}

func WrapQueryEvent(ev *replication.QueryEvent) *QueryEvent {
	return &QueryEvent{
		ev.SlaveProxyID,
		ev.ExecutionTime,
		ev.ErrorCode,
		ev.StatusVars,
		string(ev.Schema),
		string(ev.Query),
	}
}

type GTIDEvent struct {
	CommitFlag uint8
	SID        string
	GNO        int64
}

func WrapGTIDEvent(ev *replication.GTIDEvent) *GTIDEvent {
	u, _ := uuid.FromBytes(ev.SID)
	return &GTIDEvent{
		ev.CommitFlag,
		u.String(),
		ev.GNO,
	}
}

type BeginLoadQueryEvent struct {
	FileID    uint32
	BlockData string
}

func WrapBeginLoadQueryEvent(
	ev *replication.BeginLoadQueryEvent,
) *BeginLoadQueryEvent {
	return &BeginLoadQueryEvent{
		ev.FileID,
		string(ev.BlockData),
	}
}

type MariadbAnnotaeRowsEvent struct {
	Query string
}

func WrapMariadbAnnotaeRowsEvent(
	ev *replication.MariadbAnnotaeRowsEvent,
) *MariadbAnnotaeRowsEvent {
	return &MariadbAnnotaeRowsEvent{string(ev.Query)}
}

type MariadbBinlogCheckPointEvent struct {
	Info string
}

func WrapMariadbBinlogCheckPointEvent(
	ev *replication.MariadbBinlogCheckPointEvent,
) *MariadbBinlogCheckPointEvent {
	return &MariadbBinlogCheckPointEvent{string(ev.Info)}
}

type TableMapEvent struct {
	TableID     uint64
	Flags       uint16
	Schema      string
	Table       string
	ColumnCount uint64
	ColumnType  []string
	ColumnMeta  []uint16
	NullBitmap  []byte
}

func decodeColumnType(raw []byte) []string {
	colType := make([]string, len(raw))
	for i, t := range raw {
		switch t {
		case mysql.MYSQL_TYPE_DECIMAL:
			colType[i] = "DECIMAL"
		case mysql.MYSQL_TYPE_TINY:
			colType[i] = "TINY"
		case mysql.MYSQL_TYPE_SHORT:
			colType[i] = "SHORT"
		case mysql.MYSQL_TYPE_LONG:
			colType[i] = "LONG"
		case mysql.MYSQL_TYPE_FLOAT:
			colType[i] = "FLOAT"
		case mysql.MYSQL_TYPE_DOUBLE:
			colType[i] = "DOUBLE"
		case mysql.MYSQL_TYPE_NULL:
			colType[i] = "NULL"
		case mysql.MYSQL_TYPE_TIMESTAMP:
			colType[i] = "TIMESTAMP"
		case mysql.MYSQL_TYPE_LONGLONG:
			colType[i] = "LONGLONG"
		case mysql.MYSQL_TYPE_INT24:
			colType[i] = "INT24"
		case mysql.MYSQL_TYPE_DATE:
			colType[i] = "DATE"
		case mysql.MYSQL_TYPE_TIME:
			colType[i] = "TIME"
		case mysql.MYSQL_TYPE_DATETIME:
			colType[i] = "DATETIME"
		case mysql.MYSQL_TYPE_YEAR:
			colType[i] = "YEAR"
		case mysql.MYSQL_TYPE_NEWDATE:
			colType[i] = "NEWDATE"
		case mysql.MYSQL_TYPE_VARCHAR:
			colType[i] = "VARCHAR"
		case mysql.MYSQL_TYPE_BIT:
			colType[i] = "BIT"
		case mysql.MYSQL_TYPE_TIMESTAMP2:
			colType[i] = "TIMESTAMP"
		case mysql.MYSQL_TYPE_DATETIME2:
			colType[i] = "DATETIME"
		case mysql.MYSQL_TYPE_TIME2:
			colType[i] = "TIME"
		case mysql.MYSQL_TYPE_NEWDECIMAL:
			colType[i] = "NEWDECIMAL"
		case mysql.MYSQL_TYPE_ENUM:
			colType[i] = "ENUM"
		case mysql.MYSQL_TYPE_SET:
			colType[i] = "SET"
		case mysql.MYSQL_TYPE_TINY_BLOB:
			colType[i] = "TINY_BLOB"
		case mysql.MYSQL_TYPE_MEDIUM_BLOB:
			colType[i] = "MEDIUM_BLOB"
		case mysql.MYSQL_TYPE_LONG_BLOB:
			colType[i] = "LONG_BLOB"
		case mysql.MYSQL_TYPE_BLOB:
			colType[i] = "BLOB"
		case mysql.MYSQL_TYPE_VAR_STRING:
			colType[i] = "VAR_STRING"
		case mysql.MYSQL_TYPE_STRING:
			colType[i] = "STRING"
		case mysql.MYSQL_TYPE_GEOMETRY:
			colType[i] = "GEOMETRY"
		default:
			colType[i] = "UNKNOWN"
		}
	}
	return colType
}

func WrapTableMapEvent(ev *replication.TableMapEvent) *TableMapEvent {
	return &TableMapEvent{
		ev.TableID,
		ev.Flags,
		string(ev.Schema),
		string(ev.Table),
		ev.ColumnCount,
		decodeColumnType(ev.ColumnType),
		ev.ColumnMeta,
		ev.NullBitmap,
	}
}

type RowsEvent struct {
	Version       int
	Table         *TableMapEvent
	TableID       uint64
	Flags         uint16
	ExtraData     string
	ColumnCount   uint64
	ColumnBitmap1 []byte
	ColumnBitmap2 []byte
	Rows          [][]interface{}
}

func WrapRowsEvent(ev *replication.RowsEvent) *RowsEvent {
	return &RowsEvent{
		ev.Version,
		WrapTableMapEvent(ev.Table),
		ev.TableID,
		ev.Flags,
		string(ev.ExtraData),
		ev.ColumnCount,
		ev.ColumnBitmap1,
		ev.ColumnBitmap2,
		ev.Rows,
	}
}

type RowsQueryEvent struct {
	Query string
}

func WrapRowsQueryEvent(ev *replication.RowsQueryEvent) *RowsQueryEvent {
	return &RowsQueryEvent{string(ev.Query)}
}
